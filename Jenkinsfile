/*
 * This is a multibranch workflow file for defining how this project should be
 * built, tested and deployed
 */

node {
    stage 'Clean workspace'
    /* Running on a fresh Docker instance makes this redundant, but just in
     * case the host isn't configured to give us a new Docker image for every
     * build, make sure we clean things before we do anything
     */
    deleteDir()


    stage 'Checkout source'
    /*
     * Represents the SCM configuration in a "Workflow from SCM" project build. Use checkout
     * scm to check out sources matching Jenkinsfile with the SCM details from
     * the build that is executing this Jenkinsfile.
     *
     * when not in multibranch: https://issues.jenkins-ci.org/browse/JENKINS-31386
     */
    checkout scm


    stage 'Build and test'
    /* if we can't install everything we need for Ruby in less than 15 minutes
     * we might as well just give up
     */
    timeout(30) {
        sh './gradlew -iS'
    }

    stage 'Capture test results and artifacts'
    step([$class: 'JUnitResultArchiver', testResults: 'build/test-results/**/*.xml'])
    step([$class: 'ArtifactArchiver', artifacts: 'build/libs/*.jar', fingerprint: true])
}

// vim: ft=groovy
