# ================= OLLAMA CONFIG =================
OLLAMA_BASE_URL = "https://sprightliest-stevie-interlineally.ngrok-free.dev/"
OLLAMA_MODEL = "Gemma3:12b"

# ================= PROCESSING =================
BATCH_SIZE = 20
DELAY_BETWEEN_REQUESTS = 0.4
MAX_RETRIES = 3
RETRY_DELAY = 2

# ================= FILE PATHS =================
INPUT_EXCEL = "input/providers_input.xlsx"
OUTPUT_EXCEL = "output/providers_cleaned.xlsx"
PROGRESS_FILE = "logs/progress.json"
DELAY_BETWEEN_ROWS = 0.3
