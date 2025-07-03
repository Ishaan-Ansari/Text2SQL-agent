from colorama import init, Fore, Back, Style
from datetime import datetime

init(autoreset=True)  # Initialize Colorama

class PrintManager:
    @staticmethod
    def section_header(title):
        """Print section header"""
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}== {title}")
        print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")

    @staticmethod
    def subsection(title):
        """Print subsection title"""
        print(f"\n{Fore.BLUE}{'-'*40}")
        print(f"{Fore.BLUE}-- {title}")
        print(f"{Fore.BLUE}{'-'*40}{Style.RESET_ALL}\n")

    @staticmethod
    def success(message):
        """Print success message"""
        print(f"{Fore.GREEN}✓ {message}{Style.RESET_ALL}")

    @staticmethod
    def error(message):
        """Print error message"""
        print(f"{Fore.RED}✗ ERROR: {message}{Style.RESET_ALL}")

    @staticmethod
    def warning(message):
        """Print warning message"""
        print(f"{Fore.YELLOW}⚠ WARNING: {message}{Style.RESET_ALL}")

    @staticmethod
    def info(message):
        """Print info message"""
        print(f"{Fore.WHITE}ℹ {message}{Style.RESET_ALL}")

    @staticmethod
    def security(message, is_safe=True):
        """Print security message"""
        if is_safe:
            print(f"{Fore.GREEN}🔒 {message}{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}🔓 {message}{Style.RESET_ALL}")

    @staticmethod
    def query_result(result_text):
        """Print query result"""
        print(f"{Fore.CYAN}{result_text}{Style.RESET_ALL}")

    @staticmethod
    def performance(metrics):
        """Print performance metrics"""
        print(f"\n{Fore.MAGENTA}📊 Performance Metrics:")
        for key, value in metrics.items():
            print(f"{Fore.MAGENTA}   {key}: {value}{Style.RESET_ALL}")

    @staticmethod
    def timestamp():
        """Print timestamp"""
        return f"[{datetime.now().strftime('%H:%M:%S')}]"
