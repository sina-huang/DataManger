import re

league_name = "International      /  Int. Friendly Games"
clean_league_name = re.sub(r'\s*/\s*', '/', league_name)
print(clean_league_name)  # 输出: "International/Int. Friendly Games"
