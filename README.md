# Loggerservice
**Loggerservice** is a plug and play microservice for streaming and analyzing all docker logs via OpenSearch and Dashboards
<br><br><br>

## Features
- Auto-grab all docker logs on host machine
- Query and analysis using OpenSearch
- Visualization with Dashboards
  <br><br><br>

## Technology Stack
- Java 17+
- Spring Boot
- OpenSearch
- Dashboards
- Docker & Docker Compose
  <br><br><br>

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
  <br><br>

### Running the Application
Clone the repository and start the service using Docker Compose:

```
git clone https://github.com/StrangeQuark/loggerservice.git
cd loggerservice
docker-compose up --build
```
<br>

### Environment Variables
The `.env` file is required to provide necessary configuration such as encryption secrets and database credentials. Default values are provided in `.env` file so the application can run out-of-the-box for testing.

⚠️ **Warning**: Do not deploy this application to production without properly changing your environment variables. The provided `.env` is not safe to use past local deployments!
<br><br>

## Deployment
This project includes a `Jenkinsfile` for use in CI/CD pipelines. Jenkins must be configured with:

- Docker support
- Secrets or environment variables for configuration
- Access to any relevant private repositories, if needed
  <br><br>

## License
This project is licensed under the GNU General Public License. See `LICENSE.md` for details.
<br><br>

## Contributing
Contributions are welcome! Feel free to open issues or submit pull requests.
