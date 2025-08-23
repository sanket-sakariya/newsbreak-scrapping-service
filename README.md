# NewsBreak Scraper API

A comprehensive FastAPI-based web scraping system for NewsBreak.com with cyclic workflow using RabbitMQ queues and PostgreSQL storage.

## Features

- **Cyclic URL Discovery**: Automatically discovers and scrapes NewsBreak URLs in a continuous loop
- **Queue-based Architecture**: Uses RabbitMQ for reliable message queuing and processing
- **Auto-scaling Workers**: Celery-based worker system that can scale automatically
- **Dual Database Design**: Separate databases for URLs and scraped data
- **Dead Letter Queue**: Handles failed scraping attempts with retry mechanism
- **Real-time Monitoring**: API endpoints for monitoring scraping progress and queue status

## Architecture

The system follows a microservices architecture with the following components:

1. **URL Feeder**: Continuously feeds URLs from database to scraper queue
2. **Scraper Workers**: Auto-scaling workers that consume URLs and extract data
3. **Data Workers**: Process extracted data and store in PostgreSQL
4. **URL Workers**: Handle discovered URLs and store in database
5. **Queue Management**: RabbitMQ with DLX for failed message handling

## Workflow

```
1. Initial URLs → Scraper Queue
2. Scraper Workers → Extract Data + Discover URLs
3. Extracted Data → Data Queue → Data Workers → PostgreSQL
4. Discovered URLs → URL Queue → URL Workers → Database
5. New URLs → Scraper Queue (Cyclic Process)
6. Failed URLs → DLX Queue → Retry Mechanism
```

## Database Structure

### NewsBreak URLs Database
- **urls**: Stores discovered URLs with metadata

### NewsBreak Data Database
- **newsbreak_data**: Main data table with scraped content
- **source_data**: News sources
- **city_data**: Geographic locations
- **text_category_data**: Content categorization
- **nf_entities_data**: Named entities
- **nf_tags_data**: Content tags

## Installation

### Prerequisites
- Python 3.11+
- PostgreSQL 15+
- RabbitMQ 3.8+
- Docker (optional)

### Local Development

1. Clone the repository:
```bash
git clone <repository-url>
cd newsbreak-scraper-api
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
make install
make dev
```

4. Set up environment variables:
```bash
cp env.example .env
# Edit .env with your database and RabbitMQ credentials
```

5. Run the application:
```bash
make run-dev
```

### Docker Deployment

1. Build and run with Docker Compose:
```bash
make docker-build
make docker-run
```

2. Stop services:
```bash
make docker-stop
```

## Configuration

Key configuration options in `.env`:

- `DATABASE_URL_1`: NewsBreak data database connection
- `DATABASE_URL_2`: NewsBreak URLs database connection
- `RABBITMQ_URL`: RabbitMQ connection string
- `WORKER_COUNT`: Number of scraper workers
- `BATCH_SIZE`: Batch size for processing
- `FEED_INTERVAL`: URL feeding interval in seconds

## API Endpoints

### Scraper Management
- `POST /api/v1/scraper/start` - Start scraping process
- `POST /api/v1/scraper/stop` - Stop scraping process
- `GET /api/v1/scraper/status` - Get scraping status

### Queue Management
- `GET /api/v1/queues/` - Get queue statistics
- `DELETE /api/v1/queues/{queue_name}/purge` - Purge queue

### Data Access
- `GET /api/v1/newsbreak/data` - Get scraped data
- `GET /api/v1/newsbreak/data/{id}` - Get specific data record

### URL Management
- `GET /api/v1/newsbreak/urls` - Get collected URLs
- `POST /api/v1/newsbreak/urls` - Add URLs to queue
- `POST /api/v1/newsbreak/urls/deduplicate` - Run deduplication

### Health Check
- `GET /api/v1/health` - System health status

## Usage

### Starting the Scraper

```bash
curl -X POST "http://localhost:8000/api/v1/scraper/start" \
  -H "Content-Type: application/json" \
  -d '{
    "initial_urls": ["https://www.newsbreak.com"],
    "worker_count": 5,
    "batch_size": 100
  }'
```

### Monitoring Progress

```bash
curl "http://localhost:8000/api/v1/scraper/status"
```

### Viewing Scraped Data

```bash
curl "http://localhost:8000/api/v1/newsbreak/data?page=1&limit=10"
```

## Development

### Code Quality

```bash
make format    # Format code with Black
make lint      # Run linting checks
make test      # Run tests
```

### Project Structure

```
app/
├── api/                    # API layer
│   ├── v1/                # API version 1
│   │   ├── endpoints/     # API endpoints
│   │   └── router.py      # Main router
├── core/                   # Core configuration
│   ├── config.py          # Settings
│   ├── database.py        # Database management
│   ├── workers.py         # Worker classes
│   └── logging.py         # Logging configuration
├── model/                  # Database models
├── schema/                 # Pydantic schemas
└── main.py                # FastAPI application
```

## Monitoring

- **API Documentation**: Available at `/docs` (Swagger UI)
- **Health Check**: `/api/v1/health` endpoint
- **Queue Status**: `/api/v1/queues/` endpoint
- **Scraping Progress**: `/api/v1/scraper/status` endpoint

## Troubleshooting

### Common Issues

1. **Database Connection**: Ensure PostgreSQL is running and credentials are correct
2. **RabbitMQ Connection**: Check RabbitMQ service status and connection string
3. **Worker Issues**: Check logs for worker errors and queue status
4. **Memory Issues**: Adjust worker count and batch size based on system resources

### Logs

Application logs are written to:
- `app.log` - General application logs
- `error.log` - Error logs only
- Console output for development

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For support and questions, please open an issue in the repository.
