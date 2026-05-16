import logging


class DiscordLogger(logging.Handler):
    def __init__(self, client, channel, level) -> None:
        super().__init__(level)
        self.client = client
        self.channel = channel

    def emit(self, record: logging.LogRecord):
        msg_text = record.getMessage()
        if len(msg_text) > 3900:
            msg_text = msg_text[:3900] + "\n...[truncated]"
        message = "**{}**\n```\n{}\n```".format(record.levelname, msg_text)

        self.client.loop.create_task(self.channel.send(message))
