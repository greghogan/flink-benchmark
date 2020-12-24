## Compression

zpaq compression [1] was chosen for its performance, reliability, and speed.
split was used to break up large files.

The files were compressed using zpaq v7.15 installed from GNU Guix v1.2.0 [2].

[1] http://mattmahoney.net/dc/zpaq.html
[2] https://guix.gnu.org/

## Commands

The following command will decompress all files from a subdirectory:

`find . -type f -name "*.zpaq" -execdir zpaq x {} -threads 1 \; -delete`

The following command was used to convert files from the original xz 
compression to zpaq:

`find . -type f -name "*.xz" -execdir bash -c 'unxz "$0" ; zpaq a
"${0%.xz}.zpaq" "${0%.xz}" -method 5 -threads 1 ; rm "${0%.xz}"' {} \;`

Either command could be configured to run multi-threaded or (using xargs) to 
operate in parallel.

The following command was used to split files:

`split -b 100000000 statsd_server.log.zpaq statsd_server.log.zpaq.split.`
