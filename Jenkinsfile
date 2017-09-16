properties([
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

node {
    ansiColor('xterm') {
        withCredentials([
            string(credentialsId: 'artifactory-user', variable: 'secret'),
            string(credentialsId: 'packer-user', variable: 'CLIENT_SECRET')
        ]) {

            stage('Checkout') {
                checkout scm
            }
            stage('Build') {
                withMaven(jdk: 'Default', maven: 'Default') {
                    sh 'mvn clean compile test'
                }
            }
            stage('Publish') {
                withMaven(jdk: 'Default', maven: 'Default') {
                    sh 'mvn deploy'
                }
            }
        }
    }
}
