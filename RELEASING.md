# Publishing to Maven Central via Sonatype

First, [follow the instructions for installing GPG and creating a key file](http://central.sonatype.org/pages/working-with-pgp-signatures.html).  You will need a version of GPG < 2.1.

If this is your first time releasing, you will need to [create your JIRA account](https://issues.sonatype.org/secure/Signup!default.jspa), and then [create a JIRA ticket](https://issues.sonatype.org/secure/CreateIssue.jspa?issuetype=21&pid=10134) to request permission (for your account) to deploy artifacts to `com.wepay` (our groupId). For more information visit [OSSRH Guide](http://central.sonatype.org/pages/ossrh-guide.html).

Once you've done that, you should create a `gradle.properties` file in your `~/.gradle` directory with the following properties to avoid specifying the same information every time you want to publish:

```
signing.keyId=<YourKeyId>
signing.password=<YourPublicKeyPassword>
signing.secretKeyRingFile=<PathToYourKeyRingFile>
ossrhUsername=<your-jira-id>
ossrhPassword=<your-jira-password>
```

After that, you should be able to run the `./gradlew publish` task successfully.

To publish a snapshot to Sonatype, just make sure that the version number ends in `-SNAPSHOT`; to avoid doing so, just make sure it doesn't.

You can then [manage your builds](https://oss.sonatype.org/) (you may need to log in first). If you want to publish a release, click _Staging Repositories_ on the left sidebar under the _Build Promotion_ header, find the repository you want to release.  You will first need to _Close_ the repository (This just prevents modification of repository).  After that is completed, you will be able to _Release_ the repository.  (The location of these commands is near the top of the pane, accompanied by _Drop_, _Refresh_, and _Promote_). Your build should be published shortly afterward, although it may not be visible via Maven search for another couple of hours.
