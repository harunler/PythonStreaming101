import asyncio
import aiohttp
import json
import datetime
from datetime import timedelta
from math import acos, cos, sin

# Homi
LOCATION = [48, 7]

async def fetchContent(session, url):
  async with session.get(url) as response:
      return await response.text()

async def getNearestStations(phenomenon, group, radius):
  async with aiohttp.ClientSession() as session:
    date = datetime.datetime.utcnow().isoformat("T") + "Z"
    print(f"*** Query nearest station: radius={radius} Km, sensor type={phenomenon}, date={date} ***")
    print()
    url = f"https://api.opensensemap.org/boxes?date={date}&phenomenon={phenomenon}&grouptag={group}&minimal=true&exposure=outdoor&classify=true&format=json"
    content = await fetchContent(session, url)
    data = json.loads(content)
    result = []
    for station in data:
      loc = station["currentLocation"]
      coords = loc["coordinates"]
      dist = 6378.388 * acos(sin(LOCATION[0]) * sin(coords[0]) + cos(LOCATION[0]) * cos(coords[0]) * cos(coords[1] - LOCATION[1]))
      if dist <= radius:
        result.append((station["_id"], station["name"], dist))
    
    i = 0
    for item in sorted(result, key=lambda x: x[2]):
      i += 1
      print(f"{i:5d}:{item[2]:8.2f} Km -> {item[0]}, {item[1]}")
    print()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(getNearestStations("Temperatur", "Luftdaten", 4000))