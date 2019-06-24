## Google cloud storage with Google cloud API

Google cloud storage implementation using Google cloud API.
With this UFS module, users can access Google cloud storage using the `gsg://` scheme.
This module targets Alluxio version 1.8.1.

### Build

Run the following command under `<path>/<to>/alluxio-extensions/underfs/gsg` folder to build the GSG extension jar.

```bash
mvn package -DskipTests
```

The built jar can be found under `<path>/<to>/alluxio-extensions/underfs/gsg/target/`

### Deploy

Under Alluxio home directory, install the built GSG jar by running:

```bash
/bin/alluxio extensions install <path>/<to>/alluxio-extensions/underfs/gsg/target/alluxio-underfs-gsg-<version>.jar
```

### Mount

Integrate GSG and Alluxio requires `fs.gsg.credential.path` property 
which points to the JSON file path of your Google application credentials.

The default mode is `0700`, users can modify the mode by changing the value of `fs.gsg.default.mode`.

```bash
./bin/alluxio fs mount --option fs.gsg.credential.path=<path>/<to>/<your-google-credentials.json> /mnt/gsg gsg://<bucket>
```
