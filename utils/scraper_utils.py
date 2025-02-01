import json
import os
from typing import Any

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
)

from models.venue import Venue
from utils.data_utils import is_complete_venue, is_duplicate_venue


def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    # https://docs.crawl4ai.com/core/browser-crawler-config/
    return BrowserConfig(
        browser_type="chromium",  # Type of browser to simulate
        headless=False,  # Whether to run in headless mode (no GUI)
        verbose=True,  # Enable verbose logging
    )


def get_llm_strategy() -> LLMExtractionStrategy:
    """
    Returns the configuration for the language model extraction strategy.

    Returns:
        LLMExtractionStrategy: The settings for how to extract data using LLM.
    """
    # https://docs.crawl4ai.com/api/strategies/#llmextractionstrategy
    return LLMExtractionStrategy(
        provider="groq/deepseek-r1-distill-llama-70b",  # Name of the LLM provider
        api_token=os.getenv("GROQ_API_KEY"),  # API token for authentication
        schema=Venue.model_json_schema(),  # JSON schema of the data model
        extraction_type="schema",  # Type of extraction to perform
        instruction=(
            "Extract all venue objects with 'name', 'location', 'price', 'capacity', "
            "'rating', 'reviews', and a 1 sentence description of the venue from the "
            "following content."
        ),  # Instructions for the LLM
        input_format="markdown",  # Format of the input content
        verbose=True,  # Enable verbose logging
    )


async def fetch_page_content(
    crawler: AsyncWebCrawler,
    url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
) -> Any:
    """
    Fetches content from a given URL using the crawler.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL to fetch.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.

    Returns:
        CrawlResult: The result of the crawl operation.
    """
    return await crawler.arun(
        url=url,
        # https://docs.crawl4ai.com/core/browser-crawler-config/
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,  # Do not use cached data
            extraction_strategy=llm_strategy,  # Strategy for data extraction
            css_selector=css_selector,  # Target specific content on the page
            session_id=session_id,  # Unique session ID for the crawl
        ),
    )


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: list,
    seen_names: set,
) -> tuple[list, bool]:
    """
    Fetches and processes a single page of venue data.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        page_number (int): The page number to fetch.
        base_url (str): The base URL of the website.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.
        required_keys (list): List of required keys in the venue data.
        seen_names (set): Set of venue names that have already been seen.

    Returns:
        tuple:
            - list: A list of processed venues from the page.
            - bool: A flag indicating if a duplicate venue was found.
    """
    url = f"{base_url}?page={page_number}"
    print(f"Loading page {page_number}...")

    # Fetch page content
    result = await fetch_page_content(
        crawler, url, css_selector, llm_strategy, session_id
    )

    if not (result.success and result.extracted_content):
        print(f"Error fetching page {page_number}: {result.error_message}")
        return [], False

    # Parse extracted content
    extracted_data = json.loads(result.extracted_content)
    if not extracted_data:
        print("No more venues found on this page.")
        return [], False

    # Process venues
    complete_venues = []
    for venue in extracted_data:
        if not is_complete_venue(venue, required_keys):
            continue  # Skip incomplete venues

        if is_duplicate_venue(venue["name"], seen_names):
            print(f"Duplicate venue '{venue['name']}' found. Ending crawl.")
            return complete_venues, True  # Found duplicate, stop processing

        # Add venue to the list
        seen_names.add(venue["name"])
        complete_venues.append(venue)

    if not complete_venues:
        print("No complete venues found on this page.")
        return [], False

    print(f"Extracted {len(complete_venues)} venues from page {page_number}.")
    return complete_venues, False
