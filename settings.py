
URL = {"receiverURL" : "ws://192.166.82.38:8000/ws/some_path/",
        "senderURL": "ws://192.166.82.38:8000/ws/456/"
}

OPENROUTER_API_KEY = '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'

REDIS= {
    "host": "127.0.0.1",
    "port": 6379,
}


GPTDESC = """
1. 基本情况概述：不同的博彩平台对于同一场体育赛事的球队命名可能存在差异，常见的差异包括使用不同的语言、缩写等。我需要将这些名称标准化，完成不同平台间比赛名称的映射。
2. 具体解决办法：我将提供一个基准列表和需要对齐的数据字典，你需要根据这个列表对字典中的比赛名称进行标准化处理。
3. 发送给你的数据结构说明：
   3.1 基准列表：形如：
       ['AC Sparta Prague -- Linkopings FC  -- International Clubs / UEFA Champions League Women',
        'Maghreb AS de Fes -- Raja Beni Mellal -- Morocco / Morocco Cup', ...]，
       列表中每一项包含一个比赛名称和其所属联赛，其中，第一个 '--' 用于分隔两支参赛球队，第二个 '--' 后是联赛名称。
   3.2 需要对齐的数据：将以 JSON 格式发送，
       例如：{{'Platform': 'stake', 'gameName': 'Maghreb AS de Fes -- Raja Beni Mellal', 'leagueName': 'Morocco / Morocco Cup'}},
       其中，'Platform' 表示信息来源的博彩平台，'gameName' 表示比赛名称，'leagueName' 表示联赛名称。
4. 请充分考虑球队名称中的缩写、语言和特殊符号，以尽可能多地成功匹配。在联赛相同的情况下，球队名称的相似度可以作为匹配的参考。
5. 你需要将平台数据与基准列表进行匹配，分为匹配成功和匹配失败两种情况，并最终返回以下结构的 JSON 数据：
   {{
       'matchResult': 'matchSuccess' or 'matchFail',
       'successData': {{
           'nameInStandardName_list': 'nomatch' or <standard_list game name>,
           'matchName': 'nomatch' or <platform_data game name>,
       }},
       'OriginalName': <game name in the Platform>
   }}
   注意，<standard_list game name> 和 <platform_data game name> 是你需要返回的对应名称，而非原字符串。
   nameInStandardName_list 是在标准列表中找到的对应项，matchName是nameInStandardName_list去除联赛名称和--之后的字符串。
   如果是匹配失败，则返回matchFail即可，如果是匹配成功，matchName为nameInStandardName_list去除联赛名称之后的字符串。因为我需要知道具体是哪一场比赛匹配成功。

6. 具体输入的真实数据如下：
    Benchmark List：{standard_list}
    Platform Data：{platform_data}
7. 注意事项：请严格按照我的要求进行操作，仅返回我要求的 JSON 结构，不要包含任何解释、提示或说明。
"""




GPT4 = "openai/gpt-4-turbo"
GPT3_5 = "openai/gpt-3.5-turbo"