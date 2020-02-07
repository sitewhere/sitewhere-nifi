# SiteWhere Nifi Integration

This project supports integration of SiteWhere event processing into Apache Nifi flows. The project components include Nifi processors and controller services used to attach to existing SiteWhere data streams and allow for custom processing outside of the standard SiteWhere event pipeline.

## Building the Nifi Archive (NAR)

In order to build a Nifi NAR file which may be used to deploy the SiteWhere components to an existing Nifi instance, execute:

```
gradle clean build
```

This will result in a NAR file being generated in the `build` folder. Place the NAR file in the `lib` folder of a Nifi instance in order to deploy it to the instance.

## Building a Nifi Image that Includes SiteWhere Components

A Nifi Docker image which includes the SiteWhere NAR may be generated by executing:

```
gradle clean dockerImage
```

Note that, by default, the Gradle build will attempt to upload the image to a local Docker repository. Update the `gradle.properties` values to push the image to another repository. The image with SiteWhere components may be launched using the standard [Nifi Helm Chart](https://github.com/cetic/helm-nifi) by using the following stanza in the YAML:

```yaml
nifi:
  image:
    repository: sitewhere/sitewhere-nifi
    tag: 3.0.0
```
