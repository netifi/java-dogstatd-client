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
               sh 'mvn clean compile test'
            }
            stage('Publish') {
               sh 'mvn deploy'
            }
        }
    }
}
