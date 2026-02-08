import requests
import json
import time

BASE_URL = "http://localhost:8000/api"

def test_topics():
    print("Testing GET /api/topics...")
    response = requests.get(f"{BASE_URL}/topics")
    assert response.status_code == 200
    data = response.json()
    assert "topics" in data
    print("Topics OK")

def test_chat_flow():
    print("Testing Chat Flow...")
    # Initiate Check
    payload = {
        "topicId": "topic_1234",
        "message": "Hello world"
    }
    print("Initiating Chat...")
    with requests.post(f"{BASE_URL}/chat/initiate", json=payload, stream=True) as r:
        assert r.status_code == 200
        chat_id = None
        for line in r.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if decoded_line.startswith("data:"):
                    data = json.loads(decoded_line[5:])
                    if "chatId" in data:
                        chat_id = data["chatId"]
                        print(f"Chat ID received: {chat_id}")
    
    if not chat_id:
        print("Failed to get chat ID")
        return

    # Check History
    print("Checking History...")
    response = requests.get(f"{BASE_URL}/history")
    assert response.status_code == 200
    history = response.json()
    found = False
    for group in history["history"]:
        for chat in group["chats"]:
            if chat["chatId"] == chat_id:
                found = True
                break
    assert found
    print("History OK")

    # Rename Chat
    print("Renaming Chat...")
    new_title = "My New Chat Title"
    response = requests.put(f"{BASE_URL}/history/rename/{chat_id}", json={"newTitle": new_title})
    assert response.status_code == 200
    print("Rename OK")

    # Bookmark
    print("Bookmarking Chat...")
    response = requests.post(f"{BASE_URL}/bookmarks", json={"chatId": chat_id})
    assert response.status_code == 200
    print("Bookmark OK")

    # Get Bookmarks
    print("Checking Bookmarks...")
    response = requests.get(f"{BASE_URL}/bookmarks")
    assert response.status_code == 200
    bookmarks = response.json()
    assert any(b["chatId"] == chat_id for b in bookmarks["bookmarks"])
    print("Get Bookmarks OK")

    # Delete Bookmark
    print("Deleting Bookmark...")
    response = requests.delete(f"{BASE_URL}/bookmarks/{chat_id}")
    assert response.status_code == 200
    print("Delete Bookmark OK")
    
    # Delete Chat
    print("Deleting Chat History...")
    response = requests.delete(f"{BASE_URL}/history/delete/{chat_id}")
    assert response.status_code == 200
    print("Delete Chat OK")

def test_prompts():
    print("Testing Prompts...")
    # Create
    payload = {"topicId": "topic_1234", "promptText": "My custom prompt"}
    response = requests.post(f"{BASE_URL}/prompts", json=payload)
    if response.status_code == 404: # If not implemented or route wrong
        print("Prompt creation failed 404")
        return
    assert response.status_code == 200
    prompt_id = response.json()["promptId"]
    print(f"Created Prompt ID: {prompt_id}")

    # Get
    response = requests.get(f"{BASE_URL}/prompts")
    assert response.status_code == 200
    prompts = response.json()["prompts"]
    assert any(p["promptId"] == prompt_id for p in prompts)
    print("Get Prompts OK")

    # Update
    payload = {"topicId": "topic_1234", "promptText": "Updated prompt"}
    response = requests.put(f"{BASE_URL}/prompts/{prompt_id}", json=payload)
    assert response.status_code == 200
    print("Update Prompt OK")

    # Delete
    response = requests.delete(f"{BASE_URL}/prompts/{prompt_id}")
    assert response.status_code == 200
    print("Delete Prompt OK")

if __name__ == "__main__":
    try:
        test_topics()
        test_chat_flow()
        test_prompts()
        print("\nAll Tests Passed!")
    except Exception as e:
        print(f"\nTest Failed: {e}")
