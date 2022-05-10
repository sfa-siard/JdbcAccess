# JdbcAccess - SIARD 2.2 JDBC Interface to MS Access databases based on Jackcess

This package contains the JDBC interface for MS Access for SIARD Suite 2.2.


## Getting started (for devs)
For building the binaries, Java JDK (1.8 or higher) and Ant must 
have been installed. Adjust build.properties to your local configuration.


There's no need for a runnnig database in order to run the tests (in fact - there is no such thing as a running MS Access Database) - all files or provided in './testfiles'.

Run the tests with:

```shell
ant test
```


Create a release

```shell
ant release
```

## Documentation
[./doc/manual/user/index.html](./doc/manual/user/index.html) contains the manual for using the binaries.
[./doc/manual/developer/index.html](./doc/manual/user/index.html) is the manual for developers wishing
build the binaries or work on the code.

More information about the build process can be found in
[./doc/manual/developer/build.html](./doc/manual/developer/build.html)

