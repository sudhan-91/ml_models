import traceback
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from .bot_controller import botController
from .state_user_input import stateUserInput
from .user_interface_api import userInterfaceApi
# from customer_audit_service.customer_audit import customerAudit
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
# audit_obj = customerAudit()
controller = botController()
state_input = stateUserInput()
user_api = userInterfaceApi()

class inputMessage(BaseModel):
    user_id: str
    user_input: str
    usecase_name: str
    chat_id: str

class customerAudit(BaseModel):
    user_id: str
    filename: str
    file: str

class MyApp:

    def __init__(self):
        # Initialize the FastAPI app
        self.usecase = None
        self.app = FastAPI()
        # Register routes
        self._register_routes()

    def _register_routes(self):
        """Method to register the routes"""

        # self.app.add_event_handler("startup", self.load_master_agent)
        self.app.add_api_route("/health", self.health, methods=["GET"])
        self.app.add_api_route("/qm_bot/chatbot", self.chatbot, methods=["POST"])
        # self.app.add_api_route("/qm_bot/chatbot/streaming", self.chatbot_streaming, methods=["POST"])
        # self.app.add_api_route("/qm_bot/customer_audit", self.customer_audit, methods=["POST"])


        self.app.add_api_route("/api/topics", user_api.get_topics, methods=["GET"])
        self.app.add_api_route("/api/chat/initiate", self.chatbot_streaming, methods=["POST"])

        self.app.add_api_route("/api/chat", user_api.get_chat_chatid, methods=["GET"])
        self.app.add_api_route("/api/history", user_api.get_chat_timeline, methods=["GET"])
        
        self.app.add_api_route("/api/history/rename/{chatId}", user_api.rename_chat_title, methods=["PUT"])
        self.app.add_api_route("/api/history/delete/{chatId}", user_api.delete_chat_id, methods=["DELETE"])

        self.app.add_api_route("/api/like-dislike", user_api.update_response_status, methods=["POST"])
        self.app.add_api_route("/api/bookmarks", user_api.get_bookmarked, methods=["GET"])
        self.app.add_api_route("/api/bookmarks", user_api.update_bookmark, methods=["POST"])
        self.app.add_api_route("/api/bookmarks/{bookmarkId}", user_api.delete_bookmark, methods=["DELETE"])

        self.app.add_api_route("/api/prompts", user_api.get_prompts, methods=["GET"])
        self.app.add_api_route("/api/prompts", user_api.update_new_prompts, methods=["POST"])
        self.app.add_api_route("/api/prompts/{promptId}", user_api.update_existing_prompt, methods=["PUT"])
        self.app.add_api_route("/api/prompts/{promptId}", user_api.delete_prompt, methods=["DELETE"])


    async def health(self):
        return {"status":"ok"}

    async def chatbot_streaming(self, payload:inputMessage):
        response = {}
        try:
            state_input.update_user_input(payload.user_input, payload.user_id, payload.chat_id)
            if payload.usecase_name == "product_bot":
                return StreamingResponse(controller.invoke_product_streaming_bot(state_input.product_state), media_type="text/event-stream")
            elif payload.usecase_name == "shipment_bot":
                return StreamingResponse(controller.invoke_shipment_streaming_bot(state_input.product_state),
                                             media_type="text/event-stream")
            elif payload.usecase_name == "far_bot":
                return StreamingResponse(controller.invoke_far_streaming_bot(state_input.product_state),
                                         media_type="text/event-stream")
                # response = await controller.invoke_product_bot(state_input.product_state)
        except Exception:
            print(f'Error in chatbot {traceback.print_exc()}')
        return response

    async def chatbot(self, payload:inputMessage):
        response = {}
        try:
            state_input.update_user_input(payload.user_input, payload.user_id, payload.chat_id)
            if payload.usecase_name == "product_bot":
                response = await controller.invoke_product_bot(state_input.product_state, payload)
            elif payload.usecase_name == "shipment_bot":
                response = await controller.invoke_shipment_bot(state_input.product_state)
            elif payload.usecase_name == "far_bot":
                response = await controller.invoke_far_bot(state_input.product_state)

                # response = await controller.invoke_product_bot(state_input.product_state)
        except Exception:
            print(f'Error in chatbot {traceback.print_exc()}')
        return response

    '''
    async def customer_audit(self,file: UploadFile = File(...), index_name: str = Form(...)):
        try:
            content = await https://file.read()
            input_dataframe = pd.read_excel(io.BytesIO(content))
            response = audit_obj.audit_auto_fill(input_dataframe, index_name)
        except Exception:
            print(f'Error in chatbot {traceback.print_exc()}')
        return response
    '''

# Create an instance of the class to run the FastAPI application
my_app = MyApp()

# The FastAPI app instance
app = my_app.app

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],)
if __name__ == '__main__':
    uvicorn.run(app, host="https://0.0.0.0", port=8101)