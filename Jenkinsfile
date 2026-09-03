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
                        url: VAULT_URL + '/api/vault/cicd',
                        httpMode: 'POST',
                        contentType: 'APPLICATION_JSON',
                        requestBody: '{"serviceName":"loggerservice","environmentName":"e3"}',
                        customHeaders: [
                            [name: 'X-CICD-TOKEN', value: CICD_TOKEN, maskValue: true]
                        ],
                        validResponseCodes: '200'
                    )

                    writeFile file: 'loggerservice.env', text: response.content
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
