pipeline {
    agent { label 'linux-agent' }

    environment {
        VAULT_URL = credentials('VAULT_URL')
        CICD_TOKEN = credentials('LOGGER_CICD_TOKEN')
        VAULTSERVICE_ENABLED = credentials('VAULTSERVICE_ENABLED')
    }

    stages {
        stage("Retrieve Env Vars") {
            steps {
                script {
                    if(VAULTSERVICE_ENABLED == "true") {
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
        }
        stage("Deploy") {
            steps {
                script {
                    sh "docker compose --env-file loggerservice.env up --build --wait"
                    echo "All containers are up and healthy."
                }
            }
        }
    }
    post {
        always {
            sh "rm -f loggerservice.env"
            echo "Cleaned up loggerservice.env"
        }
    }
}
