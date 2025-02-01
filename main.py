import asyncio
import csv
import json
import os

from crawl4ai import AsyncWebCrawler, CacheMode, LLMExtractionStrategy
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


async def main():
    load_dotenv()

    browser_config = BrowserConfig(
        browser_type="chromium",
        headless=False,
        verbose=True,
    )  # Default browser configuration

    class Venue(BaseModel):
        name: str
        location: str
        price: str
        capacity: str
        rating: str
        reviews: str
        description: str

    llm_strategy = LLMExtractionStrategy(
        provider="groq/deepseek-r1-distill-llama-70b",
        api_token=os.getenv("GROQ_API_KEY"),
        schema=Venue.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Extract all venue objects with 'name', 'location', 'price', 'capacity', "
            "'rating', 'reviews', and a 1 sentence description of the venue from the "
            "following content."
        ),
        input_format="markdown",
        verbose=True,
    )

    session_id = "venue_crawl_session"

    async with AsyncWebCrawler(config=browser_config) as crawler:
        base_url = (
            "https://www.theknot.com/marketplace/wedding-reception-venues-atlanta-ga"
        )
        page_number = 1
        all_venues = []

        while True:
            url = f"{base_url}?page={page_number}"
            print(f"Loading page {page_number}...")
            result = await crawler.arun(
                url=url,
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    extraction_strategy=llm_strategy,
                    css_selector="[class^='info-container']",
                    session_id=session_id,
                ),
            )

            if result.success and result.extracted_content:
                extracted_data = json.loads(result.extracted_content)
                if not extracted_data:
                    print("No more venues found. Ending crawl.")
                    break

                complete_venues = [
                    venue
                    for venue in extracted_data
                    if all(
                        key in venue
                        for key in [
                            "name",
                            "price",
                            "location",
                            "capacity",
                            "rating",
                            "reviews",
                            "description",
                        ]
                    )
                ]
                all_venues.extend(complete_venues)
                print(
                    f"Extracted {len(complete_venues)} venues from page {page_number}."
                )

                page_number += 1
                # Pause for 10 seconds to be polite to the server and to not hit Groq Limit
                # of 6,000 tokens per minute
                await asyncio.sleep(10)
            else:
                print(f"Error: {result.error_message}")
                break

        if all_venues:
            # Save to CSV
            with open(
                "complete_venues.csv", mode="w", newline="", encoding="utf-8"
            ) as file:
                writer = csv.DictWriter(file, fieldnames=all_venues[0].keys())
                writer.writeheader()
                writer.writerows(all_venues)

            print(f"Saved {len(all_venues)} venues to 'complete_venues.csv'.")

        llm_strategy.show_usage()


if __name__ == "__main__":
    asyncio.run(main())
