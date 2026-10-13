"""Runtime config for Airflow DAGs and local scripts.

Reads Supabase credentials from environment variables so Docker Compose
can inject them via `.env` without committing secrets.
"""

import os

supabase_fpl_username = os.environ.get("SUPABASE_USER", "")
supabase_fpl_password = os.environ.get("SUPABASE_PASSWORD", "")
supabase_fpl_host = os.environ.get("SUPABASE_HOST", "")
supabase_fpl_port = os.environ.get("SUPABASE_PORT", "5432")
supabase_fpl_db = os.environ.get("SUPABASE_DB", "postgres")
