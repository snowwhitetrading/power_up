import os
import sys
import requests
import json

from datetime import datetime
from fuzzywuzzy import fuzz
import google.generativeai as genai
from openai import OpenAI

# Set your API keys here or use environment
SERPER_API_KEY = None
OPENAI_API_KEY = None

def load_api_keys():
    """Load API keys from multiple sources"""
    serper_key = None
    openai_key = None
    
    # Method 1: Try environment variables first
    serper_key = os.getenv("SERPER_API_KEY")
    openai_key = os.getenv("OPENAI_API_KEY")
    
    if serper_key and openai_key:
        print("✅ Found both API keys in environment variables")
        return serper_key, openai_key
    
    # Method 2: Try to read from .secrets.toml directly (in current directory)
    try:
        import toml
        secrets_path = ".secrets.toml"
        if os.path.exists(secrets_path):
            with open(secrets_path, 'r') as f:
                secrets = toml.load(f)
                serper_key = serper_key or secrets.get("SERPER_API_KEY")
                openai_key = openai_key or secrets.get("OPENAI_API_KEY")
                if serper_key or openai_key:
                    print("✅ Found API keys in .secrets.toml")
    except Exception as e:
        print(f"Could not read .secrets.toml: {e}")
    
    # Method 3: Try to read from .streamlit/secrets.toml
    try:
        import toml
        secrets_path = ".streamlit/secrets.toml"
        if os.path.exists(secrets_path):
            with open(secrets_path, 'r') as f:
                secrets = toml.load(f)
                serper_key = serper_key or secrets.get("SERPER_API_KEY")
                openai_key = openai_key or secrets.get("OPENAI_API_KEY")
                if serper_key or openai_key:
                    print("✅ Found API keys in .streamlit/secrets.toml")
    except Exception as e:
        print(f"Could not read .streamlit/secrets.toml: {e}")
    
    # Method 4: Try streamlit secrets (if available)
    try:
        import streamlit as st
        serper_key = serper_key or st.secrets.get("SERPER_API_KEY")
        openai_key = openai_key or st.secrets.get("OPENAI_API_KEY")
        if serper_key or openai_key:
            print("✅ Found API keys in Streamlit secrets")
    except Exception as e:
        print(f"Could not access Streamlit secrets: {e}")
    
    # Method 5: Check for .env file
    try:
        if os.path.exists('.env'):
            with open('.env', 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('SERPER_API_KEY='):
                        serper_key = serper_key or line.split('=', 1)[1].strip().strip('"\'')
                    elif line.startswith('OPENAI_API_KEY='):
                        openai_key = openai_key or line.split('=', 1)[1].strip().strip('"\'')
            if serper_key or openai_key:
                print("✅ Found API keys in .env file")
    except Exception as e:
        print(f"Could not read .env file: {e}")
    
    return serper_key, openai_key

# Load API keys
try:
    SERPER_API_KEY, OPENAI_API_KEY = load_api_keys()
except Exception as e:
    print(f"Error loading API keys: {e}")

# Only ask for manual input if keys are still not found
if not SERPER_API_KEY:
    print("\n❌ SERPER API key not found in any configuration file")
    print("Checked locations:")
    print("  - Environment variables (SERPER_API_KEY)")
    print("  - .streamlit/secrets.toml")
    print("  - .env file")
    print("\nPlease create a .streamlit/secrets.toml file with your key:")
    print('SERPER_API_KEY = "your_serper_key_here"')
    
    SERPER_API_KEY = input("\nEnter your Serper API Key: ").strip()

if not OPENAI_API_KEY:
    print("\n❌ openai API key not found in any configuration file")
    print("Checked locations:")
    print("  - Environment variables (OPENAI_API_KEY)")
    print("  - .streamlit/secrets.toml")
    print("  - .env file")
    print("\nPlease create a .streamlit/secrets.toml file with your key:")
    print('OPENAI_API_KEY = "your_openai_key_here"')
    
    OPENAI_API_KEY = input("\nEnter your openai API Key: ").strip()

# Initialize openai
if OPENAI_API_KEY:
    genai.configure(api_key=OPENAI_API_KEY)

def search_with_metadata(query, search_type="news", tbs=None, hl="vi", gl="vn", num=50):
    """Search using Google Serper API and return results with full metadata"""
    url = "https://google.serper.dev/search"
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    data = {
        'q': query,
        'type': search_type,
        'num': num,
        'hl': hl,
        'gl': gl
    }
    
    # Add time filter if specified
    if tbs:
        data['tbs'] = tbs
    elif search_type == "news":
        # Default: past day
        data['tbs'] = "qdr:d"
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def export_to_json(data, filename):
    """Export data to JSON file and save to data folder"""
    # Add timestamp to the data
    export_data = {
        "timestamp": datetime.now().isoformat(),
        "search_results": data
    }
    
    # Convert to JSON string
    json_str = json.dumps(export_data, indent=2, ensure_ascii=False)
    
    # Save to data folder
    data_folder = "data"
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    
    file_path = os.path.join(data_folder, filename)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
        return json_str, file_path
    except Exception as e:
        return json_str, f"Error saving file: {str(e)}"

def load_ticker_cache(ticker_symbol):
    """Load cached news articles for a specific ticker
    
    Args:
        ticker_symbol: Stock ticker symbol
        
    Returns:
        List of cached news articles, or empty list if no cache exists
    """
    cache_folder = os.path.join("data", "news_results")
    cache_file = os.path.join(cache_folder, f"{ticker_symbol}_news.json")
    
    if not os.path.exists(cache_file):
        return []
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        # Return the articles from cache
        articles = cache_data.get('articles', [])
        print(f"   📂 Loaded {len(articles)} cached articles from {cache_file}")
        return articles
    except Exception as e:
        print(f"   ⚠️  Error loading cache for {ticker_symbol}: {e}")
        return []

def save_ticker_cache(ticker_symbol, articles):
    """Save news articles to ticker-specific cache file
    
    Args:
        ticker_symbol: Stock ticker symbol
        articles: List of news articles to save
    """
    cache_folder = os.path.join("data", "news_results")
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)
    
    cache_file = os.path.join(cache_folder, f"{ticker_symbol}_news.json")
    
    cache_data = {
        "ticker": ticker_symbol,
        "last_updated": datetime.now().isoformat(),
        "total_articles": len(articles),
        "articles": articles
    }
    
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        print(f"   💾 Saved {len(articles)} articles to {cache_file}")
        return True
    except Exception as e:
        print(f"   ⚠️  Error saving cache for {ticker_symbol}: {e}")
        return False

def load_coverage_data():
    """Load ticker coverage data from coverage.json"""
    try:
        coverage_path = os.path.join("data", "coverage.json")
        with open(coverage_path, 'r', encoding='utf-8') as f:
            coverage_data = json.load(f)
        return coverage_data  # Now returns the full dictionary with ticker keys
    except Exception as e:
        print(f"Error loading coverage data: {e}")
        return None

def get_ticker_queries(ticker_symbol):
    """Generate search queries for a specific ticker using coverage.json data
    Now returns queries with their language/location settings
    Returns list of tuples: (query, hl, gl, num)
    """
    coverage_data = load_coverage_data()
    if not coverage_data:
        return []
    
    # Get ticker data from the new format
    ticker_data = coverage_data.get(ticker_symbol)
    
    if not ticker_data:
        print(f"⚠️  Warning: No data found for ticker {ticker_symbol}")
        return []
    
    name = ticker_data.get("name", "")
    description_vie = ticker_data.get("description_vie", "")
    description_eng = ticker_data.get("description_eng", "")

    if not name:
        print(f"⚠️  Warning: Incomplete data for ticker {ticker_symbol}")
        return []
    
    # Generate queries with their respective language settings
    # Format: (query, hl, gl, num)
    queries = [
        # Vietnamese news queries
        (f"Cổ phiếu {ticker_symbol}", "vi", "vn", 50),
        # Company name query - search in both Vietnamese and English
        (f"{name}", "vi", "vn", 50),
    ]
    
    return queries
                                               
def count_keyword_mentions(text, keywords):

    if not text or not keywords:
        return 0
    
    text_lower = text.lower()
    total_count = 0
    
    for keyword in keywords:
        if keyword:
            keyword_lower = keyword.lower()
            # Count occurrences of this keyword           
            total_count += text_lower.count(keyword_lower)
    
    return total_count

def filter_news_by_keyword_frequency(news_articles, ticker_symbol, min_mentions=0):

    coverage_data = load_coverage_data()
    if not coverage_data:
        return news_articles
    
    ticker_data = coverage_data.get(ticker_symbol)
    if not ticker_data:
        return news_articles
    
    # Get keywords from coverage.json
    keywords = ticker_data.get("keywords", [])
    
    # If no keywords in coverage.json, fallback to ticker and name
    if not keywords:
        ticker = ticker_data.get("ticker", "")
        name = ticker_data.get("name", "")
        keywords = [k for k in [ticker, name] if k]
    
    if not keywords:
        return news_articles
    
    filtered_articles = []
    
    for article in news_articles:
        # Combine title and snippet for keyword counting
        title = article.get('title', '')
        snippet = article.get('snippet', '')
        combined_text = f"{title} {snippet}"
        
        # Count keyword mentions
        mention_count = count_keyword_mentions(combined_text, keywords)

        # Keep article if at least one keyword is mentioned (mention_count >= 1)
        if mention_count >= 1:
            # Add metadata about keyword mentions
            article['keyword_mentions'] = mention_count
            filtered_articles.append(article)
    
    return filtered_articles

def classify_article_with_openai(article, ticker_symbol):
    """Classify a news article for importance and sentiment using OpenAI API
    
    Args:
        article: Dictionary containing article data (title, snippet, etc.)
        ticker_symbol: Stock ticker symbol for context
        
    Returns:
        Dictionary with classification results: {
            'category': 'important' or 'not important',
            'rating': 'positive', 'negative', or 'neutral'
        }
    """
    if not OPENAI_API_KEY:
        return {
            'category': 'unknown',
            'rating': 'neutral'
        }
    
    try:
        # Prepare the prompt
        title = article.get('title', '')
        snippet = article.get('snippet', '')
        source = article.get('source', '')
        
        prompt = f"""You are a financial analyst evaluating news articles for stock market relevance and sentiment.

Analyze this news article about ticker symbol "{ticker_symbol}":

Article Information:
Title: {title}
Snippet: {snippet}
Source: {source}

Classify the article on TWO dimensions:

1. CATEGORY (Importance):
   - "important": Likely to materially affect stock price, financial results, capital raising, regulation change, electricity price, operations, contracts, partnerships, regulations, or performance
   - "not important": General news, social events, awards, community activities, or other non-material information

2. RATING (Sentiment):
   - "positive": Good news, positive developments, growth, success, partnerships, awards, improved performance
   - "negative": Bad news, losses, problems, scandals, regulatory issues, declining performance
   - "neutral": Factual information without clear positive or negative implications

Provide your classification in the following JSON format:
{{
    "category": "important" or "not important",
    "rating": "positive", "negative", or "neutral"
}}

Respond with ONLY the JSON, no additional text."""

        # Call OpenAI API using new format
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.responses.create(
            model="gpt-5-mini",
            input=prompt,
        )
        
        # Parse the response
        response_text = response.output_text.strip()
        
        # Try to extract JSON from the response
        # Sometimes the model adds markdown code blocks
        if '```json' in response_text:
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif '```' in response_text:
            response_text = response_text.split('```')[1].split('```')[0].strip()
        
        result = json.loads(response_text)
        
        # Validate the result
        if 'category' not in result:
            result['category'] = 'unknown'
        if 'rating' not in result:
            result['rating'] = 'neutral'
        
        # Ensure category is valid
        if result['category'] not in ['important', 'not important']:
            result['category'] = 'unknown'
        
        # Ensure rating is valid
        if result['rating'] not in ['positive', 'negative', 'neutral']:
            result['rating'] = 'neutral'
        
        return result
        
    except Exception as e:
        print(f"   ⚠️  Error classifying article '{title[:50]}...': {str(e)}")
        return {
            'category': 'unknown',
            'rating': 'neutral'
        }

def classify_articles_batch(articles, ticker_symbol, batch_size=5):
    """Classify multiple articles with rate limiting
    
    Args:
        articles: List of article dictionaries
        ticker_symbol: Stock ticker symbol
        batch_size: Number of articles to process before pausing (for rate limiting)
        
    Returns:
        List of articles with added 'classification' field
    """
    import time
    
    classified_articles = []
    total = len(articles)
    
    print(f"\n🤖 Classifying {total} articles using openai AI...")
    
    for i, article in enumerate(articles):
        # Classify the article
        classification = classify_article_with_openai(article, ticker_symbol)
        
        # Add classification to article
        article['classification'] = classification
        classified_articles.append(article)
        
        # Print progress
        category = classification['category']
        rating = classification['rating']
        title_short = article.get('title', '')[:60]
        print(f"   [{i+1}/{total}] {category.upper()} [{rating.upper()}]: {title_short}...")
        
        # Rate limiting: pause after each batch
        if (i + 1) % batch_size == 0 and (i + 1) < total:
            print(f"   ⏸️  Pausing for rate limit...")
            time.sleep(2)  # 2 second pause between batches
    
    # Summary statistics
    important_count = sum(1 for a in classified_articles if a['classification']['category'] == 'important')
    not_important_count = sum(1 for a in classified_articles if a['classification']['category'] == 'not important')
    unknown_count = sum(1 for a in classified_articles if a['classification']['category'] == 'unknown')
    
    positive_count = sum(1 for a in classified_articles if a['classification']['rating'] == 'positive')
    negative_count = sum(1 for a in classified_articles if a['classification']['rating'] == 'negative')
    neutral_count = sum(1 for a in classified_articles if a['classification']['rating'] == 'neutral')
    
    print(f"\n📊 Classification Summary:")
    print(f"   Important: {important_count} articles")
    print(f"   Not Important: {not_important_count} articles")
    print(f"   Unknown/Error: {unknown_count} articles")
    print(f"\n📈 Rating Summary:")
    print(f"   Positive: {positive_count} articles")
    print(f"   Negative: {negative_count} articles")
    print(f"   Neutral: {neutral_count} articles")
    
    return classified_articles

def parse_relative_date(date_str):
    """Convert relative date strings to comparable values (hours ago)
    Returns a large number if cannot parse (to treat as older/unknown)
    
    Examples:
    - "1 giờ trước" -> 1 hour
    - "4 ngày trước" -> 96 hours (4 * 24)
    - "2 tuần trước" -> 336 hours (2 * 7 * 24)
    - "1 tháng trước" -> 720 hours (30 * 24)
    """
    if not date_str:
        return 999999  # Unknown dates are treated as very old
    
    date_lower = date_str.lower()
    
    try:
        # Extract number from string
        import re
        numbers = re.findall(r'\d+', date_str)
        if not numbers:
            return 999999  # No number found, treat as old
        
        num = int(numbers[0])
        
        # Convert to hours based on unit
        if 'giờ' in date_lower or 'hour' in date_lower:
            return num
        elif 'ngày' in date_lower or 'day' in date_lower:
            return num * 24
        elif 'tuần' in date_lower or 'week' in date_lower:
            return num * 7 * 24
        elif 'tháng' in date_lower or 'month' in date_lower:
            return num * 30 * 24
        elif 'năm' in date_lower or 'year' in date_lower:
            return num * 365 * 24
        else:
            return 999999  # Unknown unit, treat as old
    except:
        return 999999  # Error parsing, treat as old

def deduplicate_news(news_articles):
    """Remove duplicate news articles based on URL, title, and snippet similarity
    Uses fuzzy matching to catch similar titles with minor differences (punctuation, hyphens, etc.)
    Keeps only the oldest article when duplicates are found
    
    Args:
        news_articles: List of news article dictionaries
        
    Returns:
        Deduplicated list of news articles
    """
    if not news_articles:
        return []
    
    # Dictionary to track articles by exact URL
    articles_by_url = {}
    # List to track unique articles (for fuzzy comparison)
    unique_articles = []
    
    for article in news_articles:
        url = article.get('link', '').strip()
        title = article.get('title', '').strip()
        snippet = article.get('snippet', '').strip()
        date = article.get('date', '')
        
        is_duplicate = False
        
        # Step 1: Check for exact URL duplicates first (highest priority)
        if url and url in articles_by_url:
            existing = articles_by_url[url]
            # Keep the older one (earlier date = larger hours_ago value)
            date_hours = parse_relative_date(date)
            existing_hours = parse_relative_date(existing.get('date', ''))
            
            if date_hours > existing_hours:  # Current article is older
                articles_by_url[url] = article
                # Update in unique_articles list
                for i, a in enumerate(unique_articles):
                    if a.get('link', '') == url:
                        unique_articles[i] = article
                        break
            is_duplicate = True
        elif url:
            articles_by_url[url] = article
        
        # Step 2: Check for fuzzy title matches (if not already marked as duplicate by URL)
        if not is_duplicate and title:
            for i, existing_article in enumerate(unique_articles):
                existing_title = existing_article.get('title', '').strip()
                
                # Use fuzzy matching to catch similar titles
                # Ratio > 90 means very similar (catches punctuation differences like – vs -)
                similarity = fuzz.partial_ratio(title.lower(), existing_title.lower())

                if similarity > 60:
                    # Found a similar title - keep the older one
                    date_hours = parse_relative_date(date)
                    existing_hours = parse_relative_date(existing_article.get('date', ''))
                    
                    if date_hours > existing_hours:  # Current article is older
                        # Replace existing with this older article
                        unique_articles[i] = article
                    is_duplicate = True
                    break
        
        # Step 3: Check for fuzzy snippet matches (if not already marked as duplicate)
        if not is_duplicate and snippet:
            for i, existing_article in enumerate(unique_articles):
                existing_snippet = existing_article.get('snippet', '').strip()
                
                # Use fuzzy matching for snippets (slightly lower threshold)
                similarity = fuzz.partial_ratio(snippet.lower(), existing_snippet.lower())
                
                if similarity > 60:
                    # Found a similar snippet - keep the older one
                    date_hours = parse_relative_date(date)
                    existing_hours = parse_relative_date(existing_article.get('date', ''))
                    
                    if date_hours > existing_hours:  # Current article is older
                        # Replace existing with this older article
                        unique_articles[i] = article
                    is_duplicate = True
                    break
        
        # If not a duplicate, add to unique articles
        if not is_duplicate:
            unique_articles.append(article)
    
    return unique_articles

def combine_and_deduplicate_ticker_news(ticker_results, ticker_symbol, classify_with_ai=False):
    """Combine new search results with cached articles, deduplicate, filter, and optionally classify
    
    Args:
        ticker_results: List of query results from searches
        ticker_symbol: Stock ticker symbol
        classify_with_ai: Whether to classify articles with AI
        
    Returns:
        Dictionary with statistics and deduplicated/filtered news
    """
    
    # Load cached articles first
    cached_articles = load_ticker_cache(ticker_symbol)
    
    # Collect all NEW news from all queries
    new_news = []
    for query_result in ticker_results:
        news_articles = query_result.get('results', {}).get('news', [])
        new_news.extend(news_articles)
    
    print(f"   📊 Found {len(new_news)} new articles from search")
    print(f"   📂 Found {len(cached_articles)} cached articles")
    
    # Combine cached + new articles
    all_news = cached_articles + new_news
    print(f"   📦 Total before deduplication: {len(all_news)} articles")
    
    # Deduplicate the combined set (cached + new)
    unique_news = deduplicate_news(all_news)
    print(f"   ✨ After deduplication: {len(unique_news)} articles")

    # Apply keyword frequency filter (>= 1 mentions)
    filtered_news = filter_news_by_keyword_frequency(unique_news, ticker_symbol, min_mentions=0)
    print(f"   🔍 After keyword filter: {len(filtered_news)} articles")

    # Classify articles with AI if requested
    # Only classify NEW articles that haven't been classified yet
    if classify_with_ai and OPENAI_API_KEY:
        # Separate articles that need classification
        unclassified_articles = [a for a in filtered_news if 'classification' not in a]
        classified_articles = [a for a in filtered_news if 'classification' in a]
        
        print(f"   🤖 Articles needing classification: {len(unclassified_articles)}")
        print(f"   ✅ Already classified: {len(classified_articles)}")
        
        # Classify only unclassified articles
        if unclassified_articles:
            newly_classified = classify_articles_batch(unclassified_articles, ticker_symbol)
            # Combine with already classified
            filtered_news = classified_articles + newly_classified
        
        # Count important vs not important
        important_articles = [a for a in filtered_news if a.get('classification', {}).get('category') == 'important']
        not_important_articles = [a for a in filtered_news if a.get('classification', {}).get('category') == 'not important']
        
        # Count ratings
        positive_articles = [a for a in filtered_news if a.get('classification', {}).get('rating') == 'positive']
        negative_articles = [a for a in filtered_news if a.get('classification', {}).get('rating') == 'negative']
        neutral_articles = [a for a in filtered_news if a.get('classification', {}).get('rating') == 'neutral']
        
        result = {
            'new_articles': len(new_news),
            'cached_articles': len(cached_articles),
            'total_found': len(all_news),
            'after_deduplication': len(unique_news),
            'after_keyword_filter': len(filtered_news),
            'important': len(important_articles),
            'not_important': len(not_important_articles),
            'positive': len(positive_articles),
            'negative': len(negative_articles),
            'neutral': len(neutral_articles),
            'deduplicated_news': filtered_news
        }
    else:
        # Sort by date (newest first)
        filtered_news.sort(key=lambda x: parse_relative_date(x.get('date', '')))
        
        result = {
            'new_articles': len(new_news),
            'cached_articles': len(cached_articles),
            'total_found': len(all_news),
            'after_deduplication': len(unique_news),
            'after_keyword_filter': len(filtered_news),
            'deduplicated_news': filtered_news
        }
    
    # Save the filtered, deduplicated articles back to cache
    save_ticker_cache(ticker_symbol, filtered_news)
    
    return result

def get_all_tickers_news_with_metadata(time_period="qdr:d", classify_with_ai=False):

    coverage_data = load_coverage_data()
    if not coverage_data:
        return []
    
    # Get all tickers from the dictionary keys
    all_tickers = list(coverage_data.keys())
    
    all_ticker_results = []
    total_tickers = len(all_tickers)
    
    print(f"🗓️  Using time period: Past day")
    print(f"📊 Processing {total_tickers} tickers...")
    if classify_with_ai:
        print(f"🤖 AI classification: ENABLED")
    print()
    
    for i, ticker in enumerate(all_tickers):
        print(f"Processing ticker {i+1}/{total_tickers}: {ticker}")
        
        queries = get_ticker_queries(ticker)
        if not queries:
            print(f"   ⚠️  Skipping {ticker} - no queries generated")
            continue
            
        ticker_results = []
        
        for query_data in queries:
            # Unpack query tuple: (query, hl, gl, num)
            query, hl, gl, num = query_data
            print(f"   🔍 Searching: {query} (hl={hl}, gl={gl}, num={num})")
            result = search_with_metadata(query, "news", tbs=time_period, hl=hl, gl=gl, num=num)
            if "error" not in result:
                news_count = len(result.get('news', []))
                print(f"      ✅ Found {news_count} articles")
                ticker_results.append({
                    "query": query,
                    "query_settings": {"hl": hl, "gl": gl, "num": num},
                    "results": result
                })
            else:
                print(f"      ❌ Error: {result['error']}")
        
        # Combine, deduplicate, and filter results for this ticker
        if ticker_results:
            print(f"   🔄 Processing and caching results...")
            combined_results = combine_and_deduplicate_ticker_news(ticker_results, ticker, classify_with_ai=classify_with_ai)
            
            if classify_with_ai and 'important' in combined_results:
                print(f"   ✅ Final: {combined_results['after_keyword_filter']} articles ({combined_results['important']} important)")
            else:
                print(f"   ✅ Final: {combined_results['after_keyword_filter']} articles")
            
            all_ticker_results.append({
                "ticker": ticker,
                "queries": ticker_results,
                "combined_results": combined_results
            })
    
    return all_ticker_results



def main():
    """Main function to run news search and export for all tickers"""
    print("🚀 All Tickers News Search and Export")
    print("=" * 50)
    print("Features:")
    print("  ✓ Deduplicates overlapping news across queries")
    print("  ✓ Filters by keyword frequency (>=1 keyword mentions from coverage.json)")
    if OPENAI_API_KEY:
        print("  ✓ AI-powered business relevance classification (openai)")
    print("=" * 50)
    
    # Check for API key
    if not SERPER_API_KEY:
        print("❌ Error: SERPER_API_KEY not found!")
        print("Please set SERPER_API_KEY environment variable")
        print("Or add it to .streamlit/secrets.toml")
        return
    
    # Set time period to past day
    time_period = "qdr:d"
    print("\n🗓️  Time period: Past day")
    
    # Ask if user wants AI classification
    use_ai_classification = False
    if OPENAI_API_KEY:
        try:
            ai_choice = input("\nUse AI classification to categorize articles? (y/n) [default: n]: ").strip().lower()
            use_ai_classification = ai_choice == 'y'
        except:
            use_ai_classification = False
    
    try:
        # Get news for all tickers
        print("\n📰 Fetching news for ALL tickers from coverage.json...")
        all_results = get_all_tickers_news_with_metadata(time_period=time_period, classify_with_ai=use_ai_classification)
        
        if all_results:
            print(f"\n✅ Found results for {len(all_results)} tickers")
            
            # Count total news articles (filtered)
            total_filtered_news = 0
            total_deduplicated_news = 0
            total_new_articles = 0
            total_cached_articles = 0
            total_important = 0
            total_not_important = 0
            total_positive = 0
            total_negative = 0
            total_neutral = 0
            
            print("\n📊 Summary by ticker:")
            for ticker_result in all_results:
                ticker = ticker_result.get('ticker', 'Unknown')
                combined = ticker_result.get('combined_results', {})
                
                filtered_count = combined.get('after_keyword_filter', 0)
                deduplicated_count = combined.get('after_deduplication', 0)
                new_count = combined.get('new_articles', 0)
                cached_count = combined.get('cached_articles', 0)
                important_count = combined.get('important', 0)
                not_important_count = combined.get('not_important', 0)
                positive_count = combined.get('positive', 0)
                negative_count = combined.get('negative', 0)
                neutral_count = combined.get('neutral', 0)
                
                total_filtered_news += filtered_count
                total_deduplicated_news += deduplicated_count
                total_new_articles += new_count
                total_cached_articles += cached_count
                total_important += important_count
                total_not_important += not_important_count
                total_positive += positive_count
                total_negative += negative_count
                total_neutral += neutral_count
                
                if filtered_count > 0:
                    if use_ai_classification and important_count > 0:
                        print(f"  - {ticker}: 📥 {new_count} new + 📂 {cached_count} cached → {filtered_count} final (📊 {important_count} important | ➕{positive_count} ➖{negative_count} ⚪{neutral_count})")
                    else:
                        print(f"  - {ticker}: 📥 {new_count} new + 📂 {cached_count} cached → {filtered_count} final")
            
            print(f"\n📈 Overall Statistics:")
            print(f"   New articles from search: {total_new_articles}")
            print(f"   Cached articles loaded: {total_cached_articles}")
            print(f"   After deduplication: {total_deduplicated_news}")
            print(f"   After keyword filter (>=1 mentions): {total_filtered_news}")
            
            if use_ai_classification and total_important > 0:
                print(f"\n🤖 AI Classification Results:")
                print(f"   Important: {total_important} articles")
                print(f"   Not Important: {total_not_important} articles")
                print(f"\n📈 Sentiment Ratings:")
                print(f"   Positive: {total_positive} articles")
                print(f"   Negative: {total_negative} articles")
                print(f"   Neutral: {total_neutral} articles")
                if total_important + total_not_important > 0:
                    important_pct = (total_important / (total_important + total_not_important)) * 100
                    print(f"   Important percentage: {important_pct:.1f}%")
            
            print(f"\n💾 All ticker data saved to individual cache files (data/news_results/TICKER_news.json)")
            
            # Show sample of first filtered news article
            for ticker_result in all_results:
                filtered_news = ticker_result.get('combined_results', {}).get('deduplicated_news', [])
                if filtered_news:
                    first_news = filtered_news[0]
                    ticker = ticker_result.get('ticker', 'Unknown')
                    
                    print(f"\n📰 Sample article ({ticker}):")
                    print(f"   Title: {first_news.get('title', 'No title')}")
                    print(f"   Source: {first_news.get('source', 'Unknown')}")
                    print(f"   Date: {first_news.get('date', 'No date')}")
                    print(f"   Keyword mentions: {first_news.get('keyword_mentions', 0)}")
                    if 'classification' in first_news:
                        classification = first_news['classification']
                        print(f"   Classification: {classification.get('category', 'unknown')} | {classification.get('rating', 'neutral')}")
                    print(f"   Link: {first_news.get('link', 'No link')}")
                    break
        else:
            print("❌ No news found for any ticker")
            
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n🎉 Script completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()