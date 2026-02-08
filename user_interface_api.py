import traceback
from fastapi import Header
from typing import Optional, List
from pydantic import BaseModel
from .sql_services import sqlServices
from .configs.config_data import configData
from datetime import datetime, timedelta, date, timezone

config = configData()
sql_serv = sqlServices()

class getChat(BaseModel):
    user_id:str
    chat_id:str

class getChatTimeline(BaseModel):
    user_id:str

class RenameHistoryRequest(BaseModel):
    newTitle: str

class LikeDislikeRequest(BaseModel):
    chatId: str
    messageId: str
    feedback: str

class BookmarkRequest(BaseModel):
    chatId: str

class CreatePromptRequest(BaseModel):
    topicId: str
    promptText: str

class UpdatePromptRequest(BaseModel):
    topicId: str
    promptText: str

class userInterfaceApi(object):

    def get_time_range(self, timeline: str):
        today = date.today()

        if timeline == "TODAY":
            start = datetime.combine(today, datetime.min.time())
            end = start + timedelta(days=1)

        elif timeline == "YESTERDAY":
            start = datetime.combine(today - timedelta(days=1), datetime.min.time())
            end = start + timedelta(days=1)

        elif timeline == "LAST_7_DAYS":
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=7)

        else:
            return None, None

        return start, end

    def get_topics(self, user_id: str = Header(None)):
        response = {}
        try:
            response = config.topics
        except Exception:
            print(f'Error in get_topics {traceback.print_exc()}')
        return response

    def get_chat_chatid(self, payload:getChat, user_id: str = Header(None)):
        response = {}
        message_list = []
        query = "select * from dbo.message where chat_id = :chat_id"
        try:
            condition = {'chat_id': payload.chat_id}
            records = sql_serv.select_user_info(query, condition)
            for record in records:
                message_list.append(
                    {"sender": record['user_id'], "message": record['message'], "messageId": record['message_id'],
                     "topicId": record['topic_id']})

            response = {"chatId": payload.chat_id, "conversation": message_list}
        except Exception:
            print(f'Error in get_topics {traceback.print_exc()}')
        return response

    def get_chat_timeline(self, payload:getChatTimeline, user_id: str = Header(None)):
        query = """
        SELECT *
        FROM dbo.session
        WHERE user_id = :user_id
          AND (
                (:start IS NULL AND :end IS NULL)
                OR (created_time >= :start AND created_time < :end)
              )
        """
        response = {"history": []}
        message_list = []
        history_list = []
        time_lines = ["TODAY", "LAST_7_DAYS"]
        try:
            for timeline in time_lines:
                message_list = []
                start, end = self.get_time_range(timeline)
                condition = {'user_id': payload.user_id, 'start':start, "end": end}
                records = sql_serv.select_user_info(query, condition)
                for record in records:
                    message_list.append(
                        {"chatId": record['chat_id'], "title": record['title']})
                if len(message_list) > 0:
                    history_list.append({"timeline": timeline, "chats": message_list})
            response = {"history":history_list}
        except Exception:
            print(f'Error in get_chat_timeline {traceback.print_exc()}')
        return response

    def rename_chat_title(self, chatId: str, payload: RenameHistoryRequest, user_id: str = Header(None)):
        query = "UPDATE dbo.session SET title = :title, modified_time = GETDATE() WHERE chat_id = :chat_id AND user_id = :user_id"
        try:
            condition = {'title': payload.newTitle, 'chat_id': chatId, 'user_id': user_id}
            sql_serv.execute_update(query, condition)
            return {"status": "success", "chatId": chatId, "message": "Chat title updated successfully."}
        except Exception:
            print(f'Error in rename_chat_title {traceback.print_exc()}')
            return {"status": "error", "message": "Failed to rename chat"}

    def delete_chat_id(self, chatId: str, user_id: str = Header(None)):
        query = "UPDATE dbo.session SET deleted_time = GETDATE() WHERE chat_id = :chat_id AND user_id = :user_id"
        try:
            condition = {'chat_id': chatId, 'user_id': user_id}
            sql_serv.execute_update(query, condition)
            return {"status": "success", "id": chatId, "message": "Chat history deleted successfully."}
        except Exception:
            print(f'Error in delete_chat_id {traceback.print_exc()}')
            return {"status": "error", "message": "Failed to delete chat"}

    def update_response_status(self, payload: LikeDislikeRequest, user_id: str = Header(None)):
        query = "UPDATE dbo.message SET vote = :vote, feedback = :feedback WHERE message_id = :message_id"
        try:
            # Assuming user_id check is implicit or not needed for feedback on message ID
            condition = {'vote': payload.feedback, 'feedback': payload.feedback, 'message_id': payload.messageId} # Payload has feedback as 'like'/'dislike'. Schema has vote and feedback. 
            # Logic: vote = like/dislike? feedback = text? User payload: feedback="like".
            # Mapping: vote = payload.feedback
            sql_serv.execute_update(query, condition)
            return {"status": "success", "chatId": payload.chatId, "messageId": payload.messageId, "message": "Feedback submitted successfully."}
        except Exception:
            print(f'Error in update_response_status {traceback.print_exc()}')
            return {"status": "error", "message": "Failed to submit feedback"}

    def get_bookmarked(self, user_id: str = Header(None)):
        query = "SELECT * FROM dbo.session WHERE bookmark_status = 1 AND deleted_time IS NULL AND user_id = :user_id"
        try:
            condition = {'user_id': user_id}
            records = sql_serv.select_user_info(query, condition)
            bookmarks = []
            for r in records:
                bookmarks.append({
                    "id": r['chat_id'],
                    "chatId": r['chat_id'],
                    "title": r['title'],
                    "timestamp": r['created_time']
                })
            return {"bookmarks": bookmarks}
        except Exception:
            print(f'Error in get_bookmarked {traceback.print_exc()}')
            return {"bookmarks": []}

    def update_bookmark(self, payload: BookmarkRequest, user_id: str = Header(None)):
        query = "UPDATE dbo.session SET bookmark_status = 1 WHERE chat_id = :chat_id AND user_id = :user_id"
        try:
            condition = {'chat_id': payload.chatId, 'user_id': user_id}
            sql_serv.execute_update(query, condition)
            return {"status": "success", "id": payload.chatId, "message": "Chat bookmarked successfully."}
        except Exception:
             print(f'Error in update_bookmark {traceback.print_exc()}')
             return {"status": "error", "message": "Failed to bookmark chat"}

    def delete_bookmark(self, bookmarkId: str, user_id: str = Header(None)):
        query = "UPDATE dbo.session SET bookmark_status = 0 WHERE chat_id = :chat_id AND user_id = :user_id"
        try:
            condition = {'chat_id': bookmarkId, 'user_id': user_id}
            sql_serv.execute_update(query, condition)
            return {"status": "success", "id": bookmarkId, "message": "Bookmark removed successfully."}
        except Exception:
            print(f'Error in delete_bookmark {traceback.print_exc()}')
            return {"status": "error", "message": "Failed to delete bookmark"}

    def get_prompts(self, user_id: str = Header(None)):
        query = "SELECT * FROM dbo.prompt WHERE deleted_time IS NULL" # Assuming prompts are global or user specific? "Fetch default and user-customized prompts". 
        # If user specific, add user_id check? Schema doesn't have user_id on Prompt.
        # Assuming global for now or based on topic.
        try:
            condition = {}
            records = sql_serv.select_user_info(query, condition)
            prompts = []
            for r in records:
                 prompts.append({
                     "promptId": r['prompt_id'],
                     "topicId": r['topic_id'],
                     "prompt": r['prompt']
                 })
            return {"prompts": prompts}
        except Exception:
            print(f'Error in get_prompts {traceback.print_exc()}')
            return {"prompts": []}

    def update_new_prompts(self, payload: CreatePromptRequest, user_id: str = Header(None)):
        import uuid
        prompt_id = "prompt_" + str(uuid.uuid4())[:8]
        try:
            data = {
                'prompt_id': prompt_id,
                'prompt': payload.promptText,
                'topic_id': payload.topicId
            }
            sql_serv.insert_prompt_data(data)
            return {"status": "success", "promptId": prompt_id, "message": "Custom prompt created successfully."}
        except Exception:
            print(f'Error in update_new_prompts {traceback.print_exc()}')
            return {"status": "error", "message": "Failed to create prompt"}

    def update_existing_prompt(self, promptId: str, payload: UpdatePromptRequest, user_id: str = Header(None)):
        query = "UPDATE dbo.prompt SET prompt = :prompt, topic_id = :topic_id, modified_time = GETDATE() WHERE prompt_id = :prompt_id"
        try:
            condition = {'prompt': payload.promptText, 'topic_id': payload.topicId, 'prompt_id': promptId}
            sql_serv.execute_update(query, condition)
            return {"status": "success", "promptId": promptId, "message": "Prompt updated successfully."}
        except Exception:
            print(f'Error in update_existing_prompt {traceback.print_exc()}')
            return {"status": "error", "message": "Failed to update prompt"}

    def delete_prompt(self, promptId: str, user_id: str = Header(None)):
        query = "UPDATE dbo.prompt SET deleted_time = GETDATE() WHERE prompt_id = :prompt_id"
        try:
            condition = {'prompt_id': promptId}
            sql_serv.execute_update(query, condition)
            return {"status": "success", "promptId": promptId, "message": "Prompt deleted successfully."}
        except Exception:
             print(f'Error in delete_prompt {traceback.print_exc()}')
             return {"status": "error", "message": "Failed to delete prompt"}