This folder contains host-specific environment configurations for the `flink-hashagg` bundle.

If you do not have a host-specific environment configuration for your developer machine, clone and adapt the devhost config.

```bash
git clone \
    git@github.com:stratosphere/peelconfig.devhost.git \
    peel-wordcount-bundle/src/main/resources/config/hosts/$HOSTNAME
```

We offer an example *ACME* cluster configuration that you use as a starting point for a multi-node environment (assuming the master is named `$ENV`).

```bash
git clone \
    git@github.com:stratosphere/peelconfig.acme.git \
    peel-wordcount-bundle/src/main/resources/config/hosts/$ENV
```

For more details on when and how to use host-specific environment configurations, please consult [the Peel Manual section](http://peel-framework.org/manual/environment-configurations.html).
