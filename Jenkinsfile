pipeline {
    agent { label 'Host PC' }

    stages {
        // Integration function start: Vault
        stage("Retrieve Env Vars") {
            steps {
                script {
                    def response = httpRequest(
                        url: 'http://localhost:6020/api/vault/getVariablesByEnvironment/loggerservice/e3',
                        httpMode: 'GET',
                        acceptType: 'APPLICATION_JSON'
                    )

                    def json = readJSON text: response.content
                    def envFileContent = ''

                    json.each { entry ->
                        envFileContent += "${entry.key}=${entry.value}\n"
                    }

                    writeFile file: '.env', text: envFileContent
                    echo "Environment variables written to .env"
                }
            }
        }
        // Integration function end: Vault
        stage("Deploy") {
            steps {
                script {
                    bat "docker-compose --env-file .env up --build --wait"
                    echo "All containers are up and healthy."
                }
            }
        }
    }
}
