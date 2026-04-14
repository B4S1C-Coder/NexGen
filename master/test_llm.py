import asyncio
from openai import AsyncOpenAI

async def test():
    llm = AsyncOpenAI(api_key="123", base_url="http://localhost:8080/v1")
    try:
        response = await llm.chat.completions.create(
            model="qwen-3.5_4B_Q4_K_M",
            messages=[{"role": "user", "content": "Just say hello in valid JSON format like: {\"hello\":\"world\"}"}],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        print("RAW RESPONSE:", repr(response.choices[0].message.content))
    except Exception as e:
        print("EXCEPTION:", str(e))

asyncio.run(test())
