import json
import os
from typing import List, Set, Tuple

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
)

from models.venue import Product
from utils.data_utils import is_complete_product, is_duplicate_product


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
        schema=Product.model_json_schema(),  # JSON schema of the data model
        extraction_type="schema",  # Type of extraction to perform
        instruction=(
            """
            You are extracting product information from PetMart.vn, a Vietnamese pet supply website.
            
            Extract the following information for each pet product in JSON format:
            - name: Full product name
            - category: Product category (like "Dog Food", "Dog Accessories", etc.)
            - price: Product price including currency (VND)
            - brand: Brand name if available (can be null)
            - image_url: Full URL of the main product image
            
            For image URLs, if you see a relative path starting with "/", prepend "https://www.petmart.vn".
            
            Example response format:
            {
                "name": "Royal Canin Medium Adult",
                "category": "Dog Food",
                "price": "350.000 VND",
                "brand": "Royal Canin",
                "image_url": "https://www.petmart.vn/path/to/image.jpg"
            }
            
            Make sure to extract complete information when possible and set null values when information is not found.
            """
        ),  # Instructions for the LLM
        input_format="markdown",  # Format of the input content
        verbose=True,  # Enable verbose logging
    )


async def check_no_results(
    crawler: AsyncWebCrawler,
    url: str,
    session_id: str,
) -> bool:
    """
    Checks if the "No Results Found" message is present on the page.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL to check.
        session_id (str): The session identifier.

    Returns:
        bool: True if "No Results Found" message is found, False otherwise.
    """
    # Fetch the page without any CSS selector or extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            session_id=session_id,
        ),
    )
    

    if result.success:
        if "No Results Found" in result.cleaned_html:
            return True
    else:
        print(
            f"Error fetching page for 'No Results Found' check: {result.error_message}"
        )

    return False


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
) -> Tuple[List[dict], bool]:
    """
    Fetches and processes a single page of product data.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        page_number (int): The page number to fetch.
        base_url (str): The base URL of the website.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.
        required_keys (List[str]): List of required keys in the product data.
        seen_names (Set[str]): Set of product names that have already been seen.

    Returns:
        Tuple[List[dict], bool]:
            - List[dict]: A list of processed products from the page.
            - bool: A flag indicating if the "No Results Found" message was encountered.
    """
    url = f"{base_url}?page={page_number}"
    print(f"Loading page {page_number}...")

    # Check if "No Results Found" message is present
    no_results = await check_no_results(crawler, url, session_id)
    if no_results:
        return [], True  # No more results, signal to stop crawling

    # Fetch page content with the extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,  # Do not use cached data
            extraction_strategy=llm_strategy,  # Strategy for data extraction
            css_selector=css_selector,  # Target specific content on the page
            session_id=session_id,  # Unique session ID for the crawl
        ),
    )

    if not (result.success and result.extracted_content):
        print(f"Error fetching page {page_number}: {result.error_message}")
        return [], False

    # Parse extracted content
    extracted_data = json.loads(result.extracted_content)
    if not extracted_data:
        print(f"No products found on page {page_number}.")
        return [], False

    # After parsing extracted content
    print("Extracted data:", extracted_data)

    # Process products
    complete_products = []
    for product in extracted_data:
        # Debugging: Print each product to understand its structure
        print("Processing product:", product)

        # Ignore the 'error' key if it's False
        if product.get("error") is False:
            product.pop("error", None)  # Remove the 'error' key if it's False

        if not is_complete_product(product, required_keys):
            continue  # Skip incomplete products

        if is_duplicate_product(product["name"], seen_names):
            print(f"Duplicate product '{product['name']}' found. Skipping.")
            continue  # Skip duplicate products

        # Add product to the list
        seen_names.add(product["name"])
        complete_products.append(product)

    if not complete_products:
        print(f"No complete products found on page {page_number}.")
        return [], False

    print(f"Extracted {len(complete_products)} products from page {page_number}.")
    return complete_products, False  # Continue crawling
