import asyncio
import websockets
import json
from config import TWITCH_CHANEL_NAME, TWITCH_ACCESS_TOKEN

class TwitchChatMonitor:
    def __init__(self):
        self.channel = TWITCH_CHANEL_NAME.lower()
        self.token = TWITCH_ACCESS_TOKEN
        self.messages = []
        self.ws = None
        
    async def connect(self):
        """Connect to Twitch IRC via WebSocket"""
        uri = "wss://irc-ws.chat.twitch.tv:443"
        self.ws = await websockets.connect(uri)
        
        # Authenticate - token should not include 'oauth:' prefix if already included
        token = self.token.replace("oauth:", "")
        await self.ws.send(f"PASS oauth:{token}")
        await self.ws.send(f"NICK {self.channel}")
        
        # Request capabilities for tags (user info, etc.)
        await self.ws.send("CAP REQ :twitch.tv/tags twitch.tv/commands")
        
        # Join the channel
        await self.ws.send(f"JOIN #{self.channel}")
        
        # Wait for connection confirmation
        async for message in self.ws:
            print(f"Server: {message}")
            if "366" in message or "JOIN" in message:  # End of NAMES list or JOIN confirmation
                print(f"Successfully connected to #{self.channel}")
                break
            if "NOTICE" in message and "authentication failed" in message.lower():
                raise Exception("Authentication failed. Check your OAuth token.")
        
    async def handle_message(self, raw_message):
        """Parse and handle incoming IRC messages"""
        if raw_message.startswith("PING"):
            # Respond to PING to keep connection alive
            await self.ws.send("PONG :tmi.twitch.tv")
            return
            
        # Parse PRIVMSG (chat messages)
        if "PRIVMSG" in raw_message:
            try:
                # Twitch IRC format: :username!username@username.tmi.twitch.tv PRIVMSG #channel :message
                # Extract username (appears after : and before !)
                parts = raw_message.split("!")
                if len(parts) > 0:
                    username = parts[0].split(":")[-1].strip()
                else:
                    username = "unknown"
                
                # Extract message content (everything after "PRIVMSG #channel :")
                message_start = raw_message.find("PRIVMSG")
                if message_start != -1:
                    # Find the : after PRIVMSG #channel
                    message_start = raw_message.find(":", message_start)
                    if message_start != -1:
                        message = raw_message[message_start + 1:].strip()
                    else:
                        message = ""
                else:
                    message = ""
                
                if username and message:
                    chat_message = f"{username}: {message}"
                    self.messages.append(chat_message)
                    print(f"[{username}]: {message}")
                
            except Exception as e:
                print(f"Error parsing message: {e}")
                print(f"Raw message: {raw_message}")
                
    async def listen(self, duration=None):
        """Listen to chat messages"""
        try:
            await self.connect()
            
            start_time = asyncio.get_event_loop().time()
            
            while True:
                # Check if duration limit reached
                if duration and (asyncio.get_event_loop().time() - start_time) > duration:
                    print(f"\nReached {duration} second limit. Stopping...")
                    break
                    
                try:
                    # Receive message with timeout
                    raw_message = await asyncio.wait_for(self.ws.recv(), timeout=1.0)
                    await self.handle_message(raw_message)
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed:
                    print("Connection closed by server. Reconnecting...")
                    await asyncio.sleep(2)
                    await self.connect()
                    
        except KeyboardInterrupt:
            print("\nStopping chat monitor...")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            if self.ws:
                await self.ws.close()
                
    def get_messages(self):
        """Return the list of collected messages"""
        return self.messages


async def main():
    monitor = TwitchChatMonitor()
    
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