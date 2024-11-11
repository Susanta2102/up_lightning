import streamlit as st
import requests
import mysql.connector
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import pytz
import logging
import folium
from streamlit_folium import st_folium
from folium.plugins import HeatMap
import pandas as pd
from decimal import Decimal
import plotly.express as px
import plotly.graph_objects as go

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Streamlit configuration
st.set_page_config(page_title="UP Lightning Tracker", page_icon="⚡", layout="wide")

# Database connection function
def connect_db():
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='up_Light'
        )
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
        st.error(f"Database connection error: {err}")
        return None

# Function to create a new table
def create_new_table(conn, table_name):
    try:
        c = conn.cursor()
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                latitude DECIMAL(10, 7),
                longitude DECIMAL(10, 7),
                time DATETIME,
                flash_type VARCHAR(1),
                peak_current DECIMAL(10, 2),
                ic_height DECIMAL(10, 2),
                number_of_sensors INT,
                UNIQUE KEY unique_strike (latitude, longitude, time)
            )
        ''')
        conn.commit()
        logging.info(f"Created new table: {table_name}")
    except mysql.connector.Error as err:
        logging.error(f"Error creating new table: {err}")
        st.error(f"Error creating new table: {err}")

# Function to get the current active table
def get_active_table(conn):
    try:
        c = conn.cursor()
        c.execute("SHOW TABLES LIKE 'lightning_data%'")
        tables = c.fetchall()
        
        for table in sorted(tables, reverse=True):
            c.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = c.fetchone()[0]
            if count < 50000:
                return table[0]
        
        # If all tables are full, create a new one
        new_table = f"lightning_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        create_new_table(conn, new_table)
        return new_table
    except mysql.connector.Error as err:
        logging.error(f"Error getting active table: {err}")
        st.error(f"Error getting active table: {err}")
        return None

# Fetch lightning data from API
def fetch_lightning_data():
    url = 'http://103.251.184.43/json/generate_file.php?action=lightning'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"API request error: {e}")
        st.error(f"Failed to fetch data from API: {e}")
        return None

# Filter data for Uttar Pradesh
def filter_up_data(records):
    if not records or 'lightning_data' not in records or '10min_record' not in records['lightning_data']:
        return []
        
    min_lat, max_lat = 24.0, 28.0
    min_lon, max_lon = 77.0, 84.0
    
    return [
        record for record in records['lightning_data']['10min_record']
        if min_lat <= float(record['latitude']) <= max_lat and min_lon <= float(record['longitude']) <= max_lon
    ]

# Insert new data into MySQL
def push_data_to_db(up_records):
    conn = connect_db()
    if not conn:
        return []

    new_records = []
    try:
        active_table = get_active_table(conn)
        if not active_table:
            return []

        c = conn.cursor()
        for record in up_records:
            c.execute(f'''
                INSERT IGNORE INTO {active_table} 
                (latitude, longitude, time, flash_type, peak_current, ic_height, number_of_sensors)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                float(record['latitude']),
                float(record['longitude']),
                datetime.strptime(record['time'], '%Y-%m-%d %H:%M:%S'),
                record['flash_type'],
                float(record['peak_current']),
                float(record['ic_height']),
                int(record['number_of_sensors'])
            ))
            if c.rowcount > 0:
                new_records.append(record)
        
        conn.commit()
    except mysql.connector.Error as err:
        logging.error(f"Database insertion error: {err}")
        st.error(f"Failed to insert data into database: {err}")
    finally:
        conn.close()
    
    return new_records

# Automated task to fetch and push data every 10 minutes
def scheduled_task():
    data = fetch_lightning_data()
    if data:
        up_records = filter_up_data(data)
        new_records = push_data_to_db(up_records)
        
        if new_records:
            st.success(f"New data pushed at {datetime.now()}: {len(new_records)} records")
            logging.info(f"New data pushed: {len(new_records)} records")
        else:
            st.info(f"No new data at {datetime.now()}")
            logging.info("No new data")
    else:
        st.warning("Failed to fetch data from API")

# Create map with lightning strike locations and heatmap
def create_map(data):
    # Calculate the center of Uttar Pradesh
    center_lat, center_lon = 26.8467, 80.9462

    # Create a map centered on Uttar Pradesh
    m = folium.Map(location=[center_lat, center_lon], zoom_start=7)

    # Prepare data for heatmap
    heat_data = [[row[1], row[2]] for row in data]

    # Add heatmap layer
    HeatMap(heat_data).add_to(m)

    # Add markers for each lightning strike
    for strike in data:
        folium.CircleMarker(
            location=[strike[1], strike[2]],  # latitude and longitude
            radius=5,
            popup=f"Time: {strike[3]}<br>Flash Type: {strike[4]}<br>Peak Current: {strike[5]}<br>IC Height: {strike[6]}",
            color="#3186cc",
            fill=True,
            fillColor="#3186cc"
        ).add_to(m)

    return m

# Function to generate statistics
def generate_statistics(data):
    df = pd.DataFrame(data, columns=['id', 'latitude', 'longitude', 'time', 'flash_type', 'peak_current', 'ic_height', 'number_of_sensors'])
    
    # Convert Decimal to float
    df['peak_current'] = df['peak_current'].astype(float)
    df['ic_height'] = df['ic_height'].astype(float)
    
    stats = {
        "Total Strikes": len(df),
        "Average Peak Current": df['peak_current'].mean(),
        "Max Peak Current": df['peak_current'].max(),
        "Average IC Height": df['ic_height'].mean(),
        "Most Active Hour": df['time'].dt.hour.mode().values[0],
        "CG Strikes": len(df[df['flash_type'] == 'G']),
        "IC Strikes": len(df[df['flash_type'] == 'C'])
    }
    
    return stats

# New function to classify lightning severity
def classify_severity(peak_current):
    if peak_current < 10:
        return "Low"
    elif peak_current < 30:
        return "Moderate"
    else:
        return "Severe"

# New function to fetch weather data
def fetch_weather_data(lat, lon):
    api_key = "bd5e378503939ddaee76f12ad7a97608"  # Replace with your actual API key
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    try:
        response = requests.get(url)
        data = response.json()
        return f"{data['weather'][0]['description'].capitalize()}, {data['main']['temp']}°C"
    except Exception as e:
        logging.error(f"Weather API error: {e}")
        return "Weather data unavailable"

# Streamlit app
def main():
    st.title("⚡ Lightning Data for Uttar Pradesh")
    
    # Dark mode toggle
    if 'dark_mode' not in st.session_state:
        st.session_state.dark_mode = False
    
    dark_mode = st.sidebar.checkbox("Dark Mode", value=st.session_state.dark_mode)
    
    if dark_mode:
        st.markdown("""
        <style>
        .stApp {
            background-color: #1E1E1E;
            color: white;
        }
        </style>
        """, unsafe_allow_html=True)
    
    st.session_state.dark_mode = dark_mode
    
    if st.button("Fetch Data Now"):
        scheduled_task()
    
    st.write("Automated system is running... Data will be pushed every 10 minutes if new records are found.")
    
    # Time-based filtering
    st.sidebar.header("Filter Data")
    time_range = st.sidebar.selectbox("Select Time Range", 
                                      ["Last Hour", "Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time"])
    
    # Display recent data, map, and statistics
    st.subheader("Recent Lightning Data")
    conn = connect_db()
    if conn:
        try:
            c = conn.cursor()
            c.execute("SHOW TABLES LIKE 'lightning_data%'")
            tables = [table[0] for table in c.fetchall()]
            
            query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in tables])
            
            if time_range != "All Time":
                time_dict = {
                    "Last Hour": "1 HOUR",
                    "Last 24 Hours": "24 HOUR",
                    "Last 7 Days": "7 DAY",
                    "Last 30 Days": "30 DAY"
                }
                query += f" WHERE time >= DATE_SUB(NOW(), INTERVAL {time_dict[time_range]})"
            
            query += " ORDER BY time DESC LIMIT 1000"
            
            c.execute(query)
            data = c.fetchall()
            if data:
                df = pd.DataFrame(data, columns=['id', 'latitude', 'longitude', 'time', 'flash_type', 'peak_current', 'ic_height', 'number_of_sensors'])
                df['severity'] = df['peak_current'].apply(classify_severity)
                
                st.dataframe(df)
                
                # Create and display the map
                st.subheader("Lightning Strike Map and Heatmap")
                map = create_map(data)
                st_folium(map, width=700, height=500)
                
                # Generate and display statistics
                st.subheader("Lightning Statistics")
                stats = generate_statistics(data)
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Strikes", stats["Total Strikes"])
                    st.metric("CG Strikes", stats["CG Strikes"])
                with col2:
                    st.metric("Average Peak Current", f"{stats['Average Peak Current']:.2f} kA")
                    st.metric("IC Strikes", stats["IC Strikes"])
                with col3:
                    st.metric("Max Peak Current", f"{stats['Max Peak Current']:.2f} kA")
                    st.metric("Most Active Hour", f"{stats['Most Active Hour']:02d}:00")
                
                # Interactive charts
                st.subheader("Trend Analysis")
                df['date'] = pd.to_datetime(df['time']).dt.date
                daily_strikes = df.groupby('date').size().reset_index(name='count')
                
                fig = px.line(daily_strikes, x='date', y='count', title='Daily Lightning Strikes')
                st.plotly_chart(fig)
                
                severity_counts = df['severity'].value_counts()
                fig = px.pie(values=severity_counts.values, names=severity_counts.index, title='Lightning Severity Distribution')
                st.plotly_chart(fig)
                
                # Weather information
                st.subheader("Current Weather in Lucknow")
                weather = fetch_weather_data(26.8467, 80.9462)
                st.info(f"Current weather in Lucknow: {weather}")
                
                # User alerts for high-risk areas
                severe_strikes = df[df['severity'] == 'Severe']
                if not severe_strikes.empty:
                    st.warning("⚠️ High-risk areas detected! The following locations have experienced severe lightning strikes:")
                    for _, strike in severe_strikes.iterrows():
                        st.write(f"- Lat: {strike['latitude']}, Lon: {strike['longitude']} at {strike['time']} (Peak Current: {strike['peak_current']} kA)")
            else:
                st.info("No data available for the selected time range.")
        except mysql.connector.Error as err:
            st.error(f"Error fetching recent data: {err}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
    
    # Schedule the task
    scheduler = BackgroundScheduler(timezone=pytz.UTC)
    scheduler.add_job(scheduled_task, 'interval', minutes=10)
    scheduler.start()
    
    # Keep the script running
    try:
        st.write("Press Ctrl+C to stop the application")
        while True:
            pass
    except KeyboardInterrupt:
        scheduler.shutdown()