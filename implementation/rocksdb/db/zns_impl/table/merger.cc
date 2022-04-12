// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/zns_impl/table/merger.h"

#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "db/zns_impl/table/iterator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

namespace {
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

    void SeekForPrev(const Slice& target) override {
      Seek(target);
      Prev();
    }


  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

class LNIterator : public Iterator {
    public:
        LNIterator(Iterator* ln_iterator, ZoneFunction zone_function, void* arg);
        ~LNIterator() override;
        bool Valid() const override {
            return data_iter_.Valid();
        }
        void Seek(const Slice& target) override;
        void SeekForPrev(const Slice& target) override;
        void SeekToFirst() override;
        void SeekToLast() override;
        void Next() override;
        void Prev() override;
        Slice key() const override  {
            assert(Valid());
            return data_iter_.key();
        }
        Slice value() const override {
            assert(Valid());
            return data_iter_.value();
        }
        Status status() const override {
            return Status::OK();
        }
    private:
  void SkipEmptyDataLbasForward();
  void SkipEmptyDataLbasBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataZone();

  ZoneFunction zone_function_;
  void* arg_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_;
  std::string data_zone_handle_;
};

LNIterator::LNIterator(Iterator* ln_iterator, ZoneFunction zone_function, void* arg)
    : zone_function_(zone_function),
      arg_(arg),
      index_iter_(ln_iterator),
      data_iter_(nullptr) {}

LNIterator::~LNIterator() = default;

void LNIterator::Seek(const Slice& target) {
    index_iter_.Seek(target);
    InitDataZone();
    if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
    SkipEmptyDataLbasForward();
}

void LNIterator::SeekForPrev(const Slice& target) {
    Seek(target);
    Prev();
}

void LNIterator::SeekToFirst() {
    index_iter_.SeekToFirst();
    InitDataZone();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
    SkipEmptyDataLbasForward();
}

void LNIterator::SeekToLast() {
    index_iter_.SeekToLast();
    InitDataZone();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
    SkipEmptyDataLbasForward();
}

void LNIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataLbasForward();
}

void LNIterator::Prev() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataLbasBackward();
}

void LNIterator::SkipEmptyDataLbasForward() {
    while(data_iter_.iter() == nullptr || !data_iter_.Valid()) {
        if (!index_iter_.Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        index_iter_.Next();
        InitDataZone();
        if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
    }
}

void LNIterator::SkipEmptyDataLbasBackward() {
    while(data_iter_.iter() == nullptr || !data_iter_.Valid()) {
        if (!index_iter_.Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        index_iter_.Prev();
        InitDataZone();
        if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
    }
}

void LNIterator::SetDataIterator(Iterator* data_iter) {
    data_iter_.Set(data_iter);
}

void LNIterator::InitDataZone() {
    if (!index_iter_.Valid()) {
        SetDataIterator(nullptr);
        return;
    }
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_zone_handle_) == 0) {
        return;
    }
    Iterator* iter = (*zone_function_)(arg_, handle);
    data_zone_handle_.assign(handle.data(), handle.size());
    SetDataIterator(iter);
}

Iterator* NewLNIterator(Iterator* index_iter,
                              ZoneFunction block_function, void* arg) {
  return new LNIterator(index_iter, block_function, arg);
}

}  // namespace leveldb
