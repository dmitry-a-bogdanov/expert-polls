import logging
from enum import Enum
from typing import *

from telegram import *

log = logging.getLogger(__name__)

PollId = int


class MessageId:
    def __init__(self, chat_id: int = None, message_id: int = None, inline_message_id: str = None):
        inline = inline_message_id is not None
        chat = (chat_id is not None) and (message_id is not None)
        if inline == chat:
            raise ValueError()
        self._chat_id = chat_id
        self._message_id = message_id
        self._inline_message_id = inline_message_id

    @property
    def chat_id(self) -> int:
        return self._chat_id

    @property
    def message_id(self) -> int:
        return self._message_id

    @property
    def inline_message_id(self) -> str:
        return self._inline_message_id

    @property
    def is_inline(self):
        return self._inline_message_id is not None

    def __str__(self):
        return f'MessageId(chat={self._chat_id}, msg={self._message_id}, inline_message_id={self._inline_message_id})'

    __repr__ = __str__


class VoteType:
    PRO = 1
    CONS = 2
    PLUS_ONE = 3


class Vote:
    def __init__(self, uid: int, name: str, vote_type: VoteType):
        self._uid = uid
        self._name = name
        self._vote_type = vote_type

    @property
    def username(self):
        return self._name

    def is_pro(self):
        return self._vote_type in (VoteType.PLUS_ONE, VoteType.PRO)

    def format_user(self):
        return f'[{self._name}](tg://user?id={self._uid})'

    def show(self):
        if self._vote_type == VoteType.PLUS_ONE:
            return f'+1 (от {self.format_user()})'
        else:
            return self.format_user()

    def __str__(self):
        return f'Vote({self._vote_type}, {self._uid}, {self._name})'

    __repr__ = __str__


class Poll:
    def __init__(self, text: str):
        self._text = text

    @property
    def text(self) -> str:
        return self._text


class Emoji:
    BOAR = '\U0001F417'
    TOILET = '\U0001F6BD'
    PLUS = '\U00002795'
    MINUS = '\U00002796'


class OPTION(Enum):
    ME_TOO = ('me_too', f'{Emoji.PLUS}')
    ME_NOT = ('me_not', f'{Emoji.MINUS}')
    PLUS_ONE = ('plus_one', f'{Emoji.BOAR} человечек подскочит')
    MINUS_ONE = ('minus_one', f'{Emoji.TOILET} человечек слился')

    def __init__(self, option_id: str, text: str):
        self._option_id = option_id
        self._text = text

    @property
    def option_id(self):
        return self._option_id

    @property
    def text(self):
        return self._text

    @classmethod
    def from_string(cls, option_id: str):
        for opt in cls:
            if option_id == opt.option_id:
                return opt
        raise ValueError('not an option')


class PollExt(Poll):
    def __init__(self, poll_id: PollId, votes_pro: List[Vote], votes_cons: List[Vote], *args, **kwargs):
        super(PollExt, self).__init__(*args, **kwargs)
        self._id = poll_id
        self._votes_pro = votes_pro
        self._votes_cons = votes_cons

    @property
    def votes_pro(self) -> List[Vote]:
        return self._votes_pro

    @property
    def votes_cons(self) -> List[Vote]:
        return self._votes_cons

    @property
    def id(self) -> PollId:
        return self._id

    def __str__(self):
        return f'PollExt(id={self.id}, text={self.text}, votes_pro={self.votes_pro}, votes_cons={self.votes_cons})'

    __repr__ = __str__

    @staticmethod
    def __generate_prefix(size: int):
        yielded = 0
        if yielded == size:
            return
        without_last = size - 1
        for i in range(without_last):
            yield '├'
        yield '└'
        return

    def build_text(self):
        log.info(f'Generating text for poll {self}')

        def total_str(votes: List):
            n = len(votes)
            return f'({n})' if n > 0 else ''

        text = str(self.text)
        text += '\n\n'
        text += f'`+ `{total_str(self.votes_pro)}\n'
        for prefix, vote in zip(self.__generate_prefix(len(self.votes_pro)), self.votes_pro):
            text += '`{}` {}\n'.format(prefix, vote.show())
        text += '\n`-`\n'
        for prefix, vote in zip(self.__generate_prefix(len(self.votes_cons)), self.votes_cons):
            text += '`{}` {}\n'.format(prefix, vote.show())
        text += '\n'
        return text

    def build_markup(self) -> InlineKeyboardMarkup:
        log.info(f'Generating markup for poll {self._id}')

        def button(opt: OPTION):
            return InlineKeyboardButton(text=opt.text, callback_data=(poll_id + ':' + opt.option_id))

        poll_id = str(self._id)
        return InlineKeyboardMarkup(inline_keyboard=[
            [button(OPTION.ME_TOO), button(OPTION.ME_NOT)],
            [button(OPTION.PLUS_ONE)], [button(OPTION.MINUS_ONE)],
            [InlineKeyboardButton(text='Share', switch_inline_query=poll_id)]
        ])
