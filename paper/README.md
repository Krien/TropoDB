# Paper

This directory contains the paper for the Msc thesis that lead to TropoDB.
The most recent version of the paper is present in `./out` and should not requiring builds.

# How to build

The paper is originally written in Overleaf and the built not, therefore, not accurately reflect how the paper was truly generated.
The following approach comes close. Make sure that inkscape is installed this is a dependency for the svgs. Also we used texitopdf to generate pdf.
So make sure that that program is installed as well (in addition to a tex runtime).
Then do:

```bash
make clean && make 
```
