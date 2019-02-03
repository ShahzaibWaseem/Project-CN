# HTTP Downloader

This is a Semester Project, for the Course Computer Networks. This is a HTTP Downloader.

## Input Format
```bash
$ python3 client.py -r -n <num_connections> -i <metric_interval> -c <connection_type> -f <file_location> -o <output_location>
```
|Tag|Verbose Tag|Optional/Required|Function|
|--|--|--|--|
|-n|\-\-connections|Required|Total number of simultaneous connections|
|-i|\-\-interval|Required|Time interval in seconds between metric reporting|
|-c|\-\-type|Required|Type of connection: UDP or TCP|
|-f|\-\-url|Required|Address pointing to the file location on the web|
|-o|\-\-location|Required|Address pointing to the location where the file is downloaded|
|-r|\-\-resume|Optional|Whether to resume the existing download in progress|

In the URL (-f or \-\-url) there must be a port number. If it is just a http link then the port number is 8080.
http://www.example.com:<port_number>/path/