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
    Activity 1: Fetches current weather data for our 3 cities.
    """
    print("🌤️ Fetching weather data from Open-Meteo API...")
    results = {}
    
    for city, coords in CITIES.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current=precipitation"
        
        response = requests.get(url)
        data = response.json()
        
        rain_amount = data["current"]["precipitation"]
        results[city] = rain_amount
        print(f"   -> {city}: {rain_amount}mm of rain")
        
    return results

@activity.defn
def analyze_flood_risk(weather_data: dict) -> dict:
    """
    Activity 2: Determines flood risk based on the amount of rain.
    """
    print("🔍 Analyzing flood risk for each city...")
    risk_report = {}
    
    for city, rain in weather_data.items():
        if rain >= 50.0:
            risk = "HIGH RISK 🚨 (Severe Flooding Possible)"
        elif rain >= 15.0:
            risk = "MODERATE RISK ⚠️ (Minor Flooding Possible)"
        else:
            risk = "LOW RISK ✅ (Safe)"
            
        risk_report[city] = {"rain_mm": rain, "risk_level": risk}
        print(f"   -> {city}: {risk}")
        
    return risk_report

@activity.defn
def generate_warning_post(risk_report: dict) -> str:
    """
    Activity 3: (UPDATED) Uses Google Gemini AI to draft a social media warning!
    """
    print("🤖 AI is drafting the social media warning...")
    
    # Initialize the Gemini client
    client = genai.Client(api_key="YOUR_API_KEY_HERE")
    
    # Create the prompt instructing the AI what to write
    prompt = f"""
    You are a professional meteorologist running a social media account. 
    Write a short, engaging weather advisory based on the following live data:
    
    City Data and Flood Risks: {risk_report}
    
    Requirements:
    - Keep it under 280 characters.
    - Include relevant emojis.
    - Give a clear safety recommendation based on the data.
    """
    
    # Send the prompt to Gemini to generate the post
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
        
        # Step 1: Run the fetch activity
        weather_data = await workflow.execute_activity(
            fetch_weather_data,
            start_to_close_timeout=timedelta(seconds=15)
        )
        
        # Step 2: Pass the weather data to the risk analysis activity
        risk_report = await workflow.execute_activity(
            analyze_flood_risk,
            weather_data,
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        # Step 3: Pass the risk report to our new AI social media activity
        final_post = await workflow.execute_activity(
            generate_warning_post,
            risk_report,
            start_to_close_timeout=timedelta(seconds=15) # Increased timeout slightly for AI generation
        )
        
        print("✅ Workflow complete!")
        return final_post