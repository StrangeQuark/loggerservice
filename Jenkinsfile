pipeline {
    agent { label 'linux-agent' }

    environment {
        VAULT_URL = credentials('VAULT_URL')
        CICD_TOKEN = credentials('LOGGER_CICD_TOKEN')
        VAULTSERVICE_ENABLED = credentials('VAULTSERVICE_ENABLED')
        KUBERNETES_CICD_TOKEN = credentials('KUBERNETES_CICD_TOKEN')
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
                    def kubernetesEnabled = env.KUBERNETES_ENABLED == "true"

                    if(kubernetesEnabled) {
                        def imageRepository = env.SERVICE_IMAGE_REPOSITORY
                        def kubernetesServiceUrl = env.KUBERNETESERVICE_URL

                        if(imageRepository.isEmpty() || kubernetesServiceUrl.isEmpty())
                            error("LoggerService Kubernetes deployment configuration is incomplete")

                        def image = imageRepository + ":" + env.BUILD_NUMBER

                        withEnv(["SERVICE_IMAGE=" + image, "KUBERNETESERVICE_URL=" + kubernetesServiceUrl]) {
                            sh "docker build -t " + image + " ."
                            sh "docker push " + image
                            sh '''
                                curl --fail-with-body -X POST \\
                                    -H "X-CICD-TOKEN: $KUBERNETES_CICD_TOKEN" \\
                                    -F "serviceName=loggerservice" \\
                                    -F "image=$SERVICE_IMAGE" \\
                                    -F "environmentFile=@loggerservice.env" \\
                                    "$KUBERNETESERVICE_URL/api/kubernetes/deploy"
                            '''
                        }
                        return
                    }

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
