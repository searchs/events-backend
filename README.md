#  Event Monitoring Backend Service

The Proof of Concept  (poc) for the Event Monitoring system is built using FastAPI and sqlite3 as persistence layer.

```bash
# Download code from repo
git clone https://github.com/searchs/events-backend.git

# Create and activate a Python virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the backend service (API)
uvicorn main:app --reload


```

Test the API via the swagger docs
