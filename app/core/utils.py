import re
from urllib.parse import urlparse
from typing import Optional

def extract_domain_from_url(url: str) -> Optional[str]:
    """
    Extract domain name from URL following the specified logic:
    1. Remove http or https
    2. Remove ://
    3. Remove www if present
    4. Remove everything after the first /
    
    Args:
        url (str): The URL to extract domain from
        
    Returns:
        Optional[str]: The extracted domain name or None if invalid
        
    Examples:
        >>> extract_domain_from_url("https://www.example.com/path/to/page")
        "example.com"
        >>> extract_domain_from_url("http://example.com/path")
        "example.com"
        >>> extract_domain_from_url("https://subdomain.example.com")
        "subdomain.example.com"
    """
    if not url or not isinstance(url, str):
        return None
    
    try:
        # Parse the URL to handle edge cases properly
        parsed = urlparse(url)
        
        # Get the netloc (domain part)
        domain = parsed.netloc
        
        if not domain:
            return None
        
        # Remove www. if present
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain
        
    except Exception:
        # Fallback to regex if urlparse fails
        try:
            # Remove protocol (http:// or https://)
            domain = re.sub(r'^https?://', '', url)
            
            # Remove www. if present
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Remove everything after the first /
            domain = domain.split('/')[0]
            
            # Remove port if present
            domain = domain.split(':')[0]
            
            return domain if domain else None
            
        except Exception:
            return None
