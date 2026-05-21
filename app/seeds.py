"""Example graph seed data and seeding logic."""
import json
import logging

from app.core.db import create_graph, get_graph_by_name

log = logging.getLogger(__name__)

EXAMPLE_GRAPHS = [
    {
        "name": "📧 Daily Digest Email",
        "description": "Fetch top Hacker News stories, transform, and email a digest.",
        "graph_data": {
            "nodes": [
                {"id":"ex1_n1","type":"trigger.manual","position":{"x":60,"y":200},"data":{"label":"Start","config":{}}},
                {"id":"ex1_n2","type":"action.http_request","position":{"x":300,"y":200},"data":{"label":"Fetch HN Top Stories","config":{"url":"https://hacker-news.firebaseio.com/v0/topstories.json","method":"GET"}}},
                {"id":"ex1_n3","type":"action.transform","position":{"x":560,"y":200},"data":{"label":"Take Top 5","config":{"expression":"{'ids': input['body'][:5], 'count': len(input['body'][:5])}"}}},
                {"id":"ex1_n4","type":"action.send_email","position":{"x":820,"y":200},"data":{"label":"Send Digest","config":{"to":"you@example.com","subject":"Daily HN Digest","body":"Today's top HN story IDs: {{ex1_n3.ids}}\n\nFetched {{ex1_n3.count}} stories."}}}
            ],
            "edges": [
                {"id":"ex1_e1","source":"ex1_n1","target":"ex1_n2"},
                {"id":"ex1_e2","source":"ex1_n2","target":"ex1_n3"},
                {"id":"ex1_e3","source":"ex1_n3","target":"ex1_n4"}
            ]
        }
    },
    {
        "name": "🤖 LLM Summariser",
        "description": "Fetch a webpage, summarise with LLM, log the result. Requires OPENAI_API_KEY or use {{creds.openai}}.",
        "graph_data": {
            "nodes": [
                {"id":"ex2_n1","type":"trigger.manual","position":{"x":60,"y":200},"data":{"label":"Start","config":{}}},
                {"id":"ex2_n2","type":"action.http_request","position":{"x":300,"y":200},"data":{"label":"Fetch Content","config":{"url":"https://hacker-news.firebaseio.com/v0/item/1.json","method":"GET"}}},
                {"id":"ex2_n3","type":"action.llm_call","position":{"x":560,"y":200},"data":{"label":"Summarise","config":{"model":"gpt-4o-mini","prompt":"Summarise this in 2 sentences: {{ex2_n2.body}}","system":"You are a concise technical summariser."}}},
                {"id":"ex2_n4","type":"action.log","position":{"x":820,"y":200},"data":{"label":"Log Summary","config":{"message":"Summary: {{ex2_n3.response}} ({{ex2_n3.tokens}} tokens)"}}}
            ],
            "edges": [
                {"id":"ex2_e1","source":"ex2_n1","target":"ex2_n2"},
                {"id":"ex2_e2","source":"ex2_n2","target":"ex2_n3"},
                {"id":"ex2_e3","source":"ex2_n3","target":"ex2_n4"}
            ]
        }
    },
    {
        "name": "🔄 Loop + Notify Per Item",
        "description": "Fetch a list, filter it, loop, and notify per item.",
        "graph_data": {
            "nodes": [
                {"id":"ex3_n1","type":"trigger.manual","position":{"x":60,"y":220},"data":{"label":"Start","config":{}}},
                {"id":"ex3_n2","type":"action.http_request","position":{"x":280,"y":220},"data":{"label":"Fetch Todos","config":{"url":"https://jsonplaceholder.typicode.com/todos?_limit=10","method":"GET"}}},
                {"id":"ex3_n3","type":"action.filter","position":{"x":500,"y":220},"data":{"label":"Filter Incomplete","config":{"expression":"not item.get('completed', True)"}}},
                {"id":"ex3_n4","type":"action.loop","position":{"x":720,"y":220},"data":{"label":"Loop Items","config":{"field":"items","max_items":"20"}}},
                {"id":"ex3_n5","type":"action.log","position":{"x":960,"y":140},"data":{"label":"Log Item","config":{"message":"Todo: {{item.title}}"}}},
                {"id":"ex3_n6","type":"action.log","position":{"x":720,"y":400},"data":{"label":"Done","config":{"message":"Loop complete"}}}
            ],
            "edges": [
                {"id":"ex3_e1","source":"ex3_n1","target":"ex3_n2"},
                {"id":"ex3_e2","source":"ex3_n2","target":"ex3_n3"},
                {"id":"ex3_e3","source":"ex3_n3","target":"ex3_n4"},
                {"id":"ex3_e4","source":"ex3_n4","target":"ex3_n5","sourceHandle":"body"},
                {"id":"ex3_e5","source":"ex3_n4","target":"ex3_n6","sourceHandle":"done"}
            ]
        }
    },
    {
        "name": "⚠ Conditional Alert",
        "description": "Fetch data, check a condition, branch to alert or log.",
        "graph_data": {
            "nodes": [
                {"id":"ex4_n1","type":"trigger.manual","position":{"x":60,"y":200},"data":{"label":"Start","config":{}}},
                {"id":"ex4_n2","type":"action.http_request","position":{"x":280,"y":200},"data":{"label":"Fetch Status","config":{"url":"https://httpbin.org/status/200","method":"GET"}}},
                {"id":"ex4_n3","type":"action.condition","position":{"x":500,"y":200},"data":{"label":"Is OK?","config":{"expression":"input.get('status') == 200"}}},
                {"id":"ex4_n4","type":"action.log","position":{"x":740,"y":120},"data":{"label":"All good","config":{"message":"Service is healthy ✓"}}},
                {"id":"ex4_n5","type":"action.log","position":{"x":740,"y":300},"data":{"label":"Alert!","config":{"message":"Service may be down — status {{ex4_n2.status}}"}}}
            ],
            "edges": [
                {"id":"ex4_e1","source":"ex4_n1","target":"ex4_n2"},
                {"id":"ex4_e2","source":"ex4_n2","target":"ex4_n3"},
                {"id":"ex4_e3","source":"ex4_n3","target":"ex4_n4","sourceHandle":"true"},
                {"id":"ex4_e4","source":"ex4_n3","target":"ex4_n5","sourceHandle":"false"}
            ]
        }
    },
    {
        "name": "🐍 Python Data Transform",
        "description": "Fetch JSON data, reshape it with a Python script, and log the result.",
        "graph_data": {
            "nodes": [
                {"id":"ex5_n1","type":"trigger.manual","position":{"x":60,"y":200},"data":{"label":"Start","config":{}}},
                {"id":"ex5_n2","type":"action.http_request","position":{"x":280,"y":200},"data":{"label":"Fetch Users","config":{"url":"https://jsonplaceholder.typicode.com/users?_limit=5","method":"GET"}}},
                {"id":"ex5_n3","type":"action.run_script","position":{"x":520,"y":200},"data":{"label":"Extract Emails","config":{"script":"users = input if isinstance(input, list) else input.get('body', [])\nresult = {'emails': [u['email'] for u in users], 'count': len(users)}"}}},
                {"id":"ex5_n4","type":"action.log","position":{"x":760,"y":200},"data":{"label":"Log Emails","config":{"message":"Found {{ex5_n3.count}} users: {{ex5_n3.emails}}"}}},
                {"id":"ex5_note","type":"note","position":{"x":60,"y":360},"data":{"label":"Note","config":{"text":"Run Script node can do any Python.\nAssign your output to the 'result' variable."}}}
            ],
            "edges": [
                {"id":"ex5_e1","source":"ex5_n1","target":"ex5_n2"},
                {"id":"ex5_e2","source":"ex5_n2","target":"ex5_n3"},
                {"id":"ex5_e3","source":"ex5_n3","target":"ex5_n4"}
            ]
        }
    },
    {
        "name": "⏰ Scheduled Cron Flow",
        "description": "Runs on a cron schedule. Edit the Cron node to set your timing.",
        "graph_data": {
            "nodes": [
                {"id":"ex6_n1","type":"trigger.cron","position":{"x":60,"y":200},"data":{"label":"Every day at 9am","config":{"cron":"0 9 * * *","timezone":"UTC","description":"Daily at 9am UTC"}}},
                {"id":"ex6_n2","type":"action.http_request","position":{"x":320,"y":200},"data":{"label":"Fetch Data","config":{"url":"https://hacker-news.firebaseio.com/v0/topstories.json","method":"GET"}}},
                {"id":"ex6_n3","type":"action.log","position":{"x":580,"y":200},"data":{"label":"Log Result","config":{"message":"Scheduled run complete. First story ID: {{ex6_n2.body.0}}"}}},
                {"id":"ex6_note","type":"note","position":{"x":60,"y":360},"data":{"label":"Note","config":{"text":"Save this flow to activate the schedule.\nEdit the Cron node to change timing.\nTimezone examples: UTC, Europe/London, US/Eastern"}}}
            ],
            "edges": [
                {"id":"ex6_e1","source":"ex6_n1","target":"ex6_n2"},
                {"id":"ex6_e2","source":"ex6_n2","target":"ex6_n3"}
            ]
        }
    },
]


def seed_example_graphs() -> int:
    """Seed missing example graphs into the database. Returns count of newly seeded graphs."""
    seeded = 0
    for eg in EXAMPLE_GRAPHS:
        try:
            if not get_graph_by_name(eg["name"]):
                create_graph(eg["name"], eg["description"], json.dumps(eg["graph_data"]))
                log.info(f"Seeded example graph: {eg['name']}")
                seeded += 1
        except (ValueError, TypeError, OSError, RuntimeError) as e:
            log.warning(f"Could not seed '{eg['name']}': {e}")
    return seeded
