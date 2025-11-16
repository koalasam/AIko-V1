import asyncio
import pytchat
from config import YOUTUBE_VIDEO_ID

class YouTubeChatMonitor:
    def __init__(self):
        self.video_id = YOUTUBE_VIDEO_ID
        self.messages = []
        self.chat = None
        
    async def connect(self):
        """Connect to YouTube chat"""
        try:
            self.chat = pytchat.create(video_id=self.video_id)
            print(f"Successfully connected to YouTube video: {self.video_id}")
        except Exception as e:
            raise Exception(f"Failed to connect to YouTube chat: {e}")
        
    async def handle_message(self, chat_data):
        """Parse and handle incoming chat messages"""
        try:
            username = chat_data.author.name
            message = chat_data.message
            
            if username and message:
                chat_message = f"{username}: {message}"
                self.messages.append(chat_message)
                print(f"[{username}]: {message}")
            
        except Exception as e:
            print(f"Error parsing message: {e}")
                
    async def listen(self, duration=None):
        """Listen to chat messages"""
        try:
            await self.connect()
            
            start_time = asyncio.get_event_loop().time()
            
            while self.chat.is_alive():
                # Check if duration limit reached
                if duration and (asyncio.get_event_loop().time() - start_time) > duration:
                    print(f"\nReached {duration} second limit. Stopping...")
                    break
                    
                try:
                    # Process chat messages
                    for chat_data in self.chat.get().sync_items():
                        await self.handle_message(chat_data)
                    
                    # Small async sleep to prevent blocking
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    print(f"Error receiving message: {e}")
                    await asyncio.sleep(1)
                    
        except KeyboardInterrupt:
            print("\nStopping chat monitor...")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            if self.chat:
                self.chat.terminate()
                
    def get_messages(self):
        """Return the list of collected messages"""
        return self.messages


async def main():
    monitor = YouTubeChatMonitor()
    
    # Listen for 60 seconds (change or remove duration parameter for continuous listening)
    await monitor.listen()
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Total messages collected: {len(monitor.get_messages())}")
    print(f"{'='*50}\n")
    
    # Print all messages
    for msg in monitor.get_messages():
        print(msg)


if __name__ == "__main__":
    asyncio.run(main())