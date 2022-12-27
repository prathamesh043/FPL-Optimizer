from flask import Flask, render_template
import psycopg2
import config

try: 
    conn = psycopg2.connect(database="postgres", user=config.supabase_fpl_username, password=config.supabase_fpl_password, host="db.pdqpnebestkagqneooty.supabase.co")
    print("connected")
except:
    print ("I am unable to connect to the database")

mycursor = conn.cursor()

app = Flask(__name__)

@app.route('/')
def index():
    mycursor.execute("SELECT * FROM optimum_squads.total_points;")
    data = mycursor.fetchall()
    return render_template('index.html', data=data)