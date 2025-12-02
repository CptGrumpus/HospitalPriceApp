import asyncio
from playwright.async_api import async_playwright
import json

async def scrape_michigan_urls():
    print("--- API Spy Mode ---")
    print("1. I will launch the browser.")
    print("2. You click Michigan.")
    print("3. I will try to steal the data from the API response.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        # Network Listener
        async def handle_response(response):
            if "searchstate=MI" in response.url:
                print(f"\n>>> CAUGHT API RESPONSE: {response.url}")
                try:
                    data = await response.json()
                    
                    # Check if it's a list
                    if isinstance(data, list):
                        print(f">>> It is a LIST with {len(data)} items!")
                        print(f">>> First Item Keys: {data[0].keys()}")
                        
                        # Save it immediately
                        with open("data/michigan_hospitals_raw.json", "w") as f:
                            json.dump(data, f, indent=2)
                        print(">>> SAVED TO data/michigan_hospitals_raw.json")
                        
                    elif isinstance(data, dict):
                        print(f">>> It is a DICT. Keys: {data.keys()}")
                        with open("data/michigan_hospitals_raw.json", "w") as f:
                            json.dump(data, f, indent=2)
                        print(">>> SAVED TO data/michigan_hospitals_raw.json")
                        
                except Exception as e:
                    print(f">>> Failed to parse JSON: {e}")

        page.on("response", handle_response)
        
        print("Navigating...")
        await page.goto("https://hospitalpricingfiles.org/", timeout=60000)
        
        print("\n>>> CLICK MICHIGAN NOW! (I'll wait 60 seconds) <<<")
        await page.wait_for_timeout(60000) 
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_michigan_urls())
