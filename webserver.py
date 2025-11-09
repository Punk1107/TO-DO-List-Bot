# (Imports เดิมของคุณ... เช่น import discord)
from flask import Flask, jsonify
from threading import Thread
import logging
import os  # <--- เพิ่มอันนี้ด้วย
import socket

# =================================================================
# WEB SERVER (สำหรับหลอก Render ไม่ให้ Sleep)
# =================================================================

app = Flask(__name__)

# ปิด Log ของ Flask เพื่อไม่ให้มันตีกับ Log ของบอท
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route("/")
def home():
    """ นี่คือหน้าเว็บที่ Render จะมาสแกน """
    return jsonify({"status": "Bot is running!", "message": "This is a keep-alive endpoint."})

def run_server():
    """ รัน Flask app โดยใช้ Port ที่ Render กำหนดมาให้ """
    try:
        # (สำคัญ!) Render จะส่ง Port มาให้เราทาง $PORT (ปกติคือ 10000)
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
        print(f"[WebServer] Started successfully on port {port}")
    except Exception as e:
        print(f"[WebServer] Failed to start Flask: {e}")

def start_webserver():
    """ สั่งให้ Flask ทำงานใน thread แยก (daemon) """
    try:
        thread = Thread(target=run_server, name="FlaskWebServer", daemon=True)
        thread.start()
        print("[WebServer] Flask thread started successfully.")
    except Exception as e:
        print(f"[WebServer] Error starting Flask thread: {e}")

# =================================================================
# (จบส่วน Web Server)
# =================================================================