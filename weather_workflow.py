from temporalio import activity, workflow
from datetime import timedelta

# This tells Temporal's security guard to ignore these libraries!
with workflow.unsafe.imports_passed_through():
    import requests
    from google import genai

# These are the 3 cities we are checking (Manila, Cebu, Iligan)
CITIES = {
    "Manila": {"lat": 14.5995, "lon": 120.9842},
    "Cebu": {"lat": 10.3157, "lon": 123.8854},
    "Iligan": {"lat": 8.2280, "lon": 124.2452}
}

@activity.defn
def fetch_weather_data() -> dict:
    """
    Activity 1: Fetches current weather data (rain, temp, wind) for our 3 cities.
    """
    print("🌤️ Fetching comprehensive weather data from Open-Meteo API...")
    results = {}
    
    for city, coords in CITIES.items():
        # UPDATED: Now asking for precipitation, temperature, and wind speed!
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current=precipitation,temperature_2m,wind_speed_10m"
        
        response = requests.get(url)
        data = response.json()
        
        current = data["current"]
        rain_amount = current["precipitation"]
        temperature = current["temperature_2m"]
        wind_speed = current["wind_speed_10m"]
        
        results[city] = {
            "rain_mm": rain_amount,
            "temp_c": temperature,
            "wind_kmh": wind_speed
        }
        print(f"   -> {city}: {rain_amount}mm rain, {temperature}°C, {wind_speed} km/h wind")
        
    return results

@activity.defn
def analyze_weather_risks(weather_data: dict) -> dict:
    """
    Activity 2: Determines various risks based on rain, heat, and wind.
    """
    print("🔍 Analyzing weather hazards for each city...")
    risk_report = {}
    
    for city, stats in weather_data.items():
        hazards = []
        
        # 1. Flood Check
        if stats["rain_mm"] >= 50.0:
            hazards.append("HIGH FLOOD RISK 🚨")
        elif stats["rain_mm"] >= 15.0:
            hazards.append("MODERATE FLOOD RISK ⚠️")
            
        # 2. Heat Wave Check (Over 35°C / 95°F)
        if stats["temp_c"] >= 35.0:
            hazards.append("HEAT WAVE WARNING 🌡️")
            
        # 3. Typhoon / Strong Wind Check (Over 61 km/h)
        if stats["wind_kmh"] >= 61.0:
            hazards.append("TYPHOON / STRONG WIND WARNING 🌀")
            
        # If no hazards triggered, it's a safe day!
        if not hazards:
            hazards.append("ALL CLEAR ✅ (Safe)")
            
        risk_report[city] = {
            "data": stats,
            "hazards": hazards
        }
        print(f"   -> {city}: {', '.join(hazards)}")
        
    return risk_report

@activity.defn
def generate_warning_post(risk_report: dict) -> str:
    """
    Activity 3: Uses Google Gemini AI to draft a comprehensive social media warning!
    """
    print("🤖 AI is drafting the social media warning...")
    
    # Initialize the Gemini client (Remember to paste your key here when testing locally!)
    client = genai.Client(api_key="YOUR_API_KEY_HERE")
    
    # UPDATED: Modified the prompt to handle multiple types of weather hazards
    prompt = f"""
    You are a professional meteorologist running a social media account. 
    Write a short, engaging weather advisory based on the following live data:
    
    City Data and Weather Hazards: {risk_report}
    
    Requirements:
    - Keep it under 280 characters.
    - Include relevant emojis for heat, rain, or wind.
    - Give a clear safety recommendation based on the current hazards.
    """
    
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt
    )
    
    post = response.text
    print(f"   -> {post}")
    return post

@workflow.defn
class WeatherWarningWorkflow:
    @workflow.run
    async def run(self) -> str:
        print("🚀 Starting Weather Warning Workflow...")
        
        weather_data = await workflow.execute_activity(
            fetch_weather_data,
            start_to_close_timeout=timedelta(seconds=15)
        )
        
        # UPDATED: Call the newly renamed risk analyzer
        risk_report = await workflow.execute_activity(
            analyze_weather_risks,
            weather_data,
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        final_post = await workflow.execute_activity(
            generate_warning_post,
            risk_report,
            start_to_close_timeout=timedelta(seconds=15)
        )
        
        print("✅ Workflow complete!")
        return final_post