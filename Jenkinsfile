pipeline {
    agent { label 'linux-agent' }

    environment {
        VAULT_URL = credentials('VAULT_URL') // Integration line: Vault
        CICD_TOKEN = credentials('CICD_TOKEN') // Integration line: Vault
    }

    stages {
        // Integration function start: Vault
        stage("Retrieve Env Vars") {
            steps {
                script {
                    def response = httpRequest(
                        url: VAULT_URL + '/api/vault/cicd/loggerservice/e3',
                        httpMode: 'GET',
                        customHeaders: [
                            [name: 'X-CICD-TOKEN', value: CICD_TOKEN, maskValue: true]
                        ],
                        acceptType: 'APPLICATION_JSON'
                    )

                    def json = readJSON text: response.content
                    def envFileContent = ''

                    json.each { entry ->
                        envFileContent += "${entry.key}=${entry.value}\n"
                    }

                    writeFile file: 'loggerservice.env', text: envFileContent
                    echo "Environment variables written to loggerservice.env"
                }
            }
        }
        // Integration function end: Vault
        stage("Deploy") {
            steps {
                script {
                    sh "docker compose --env-file loggerservice.env up --build --wait"
                    echo "All containers are up and healthy."
                }
            }
        }
    }
    // Integration function start: Vault
    post {
        always {
            sh "rm -f loggerservice.env"
            echo "Cleaned up loggerservice.env"
        }
    }
    // Integration function end: Vault
}
