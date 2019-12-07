import sqlite3
import datetime
import threading

from bot_types import *


class Storage:
    def __init__(self, connection: sqlite3.Connection):
        self._lock = threading.Lock()
        self._data = dict()
        self._seq = 0
        self._db = connection  # type: sqlite3.Connection

        self._db.executescript('''
            BEGIN TRANSACTION;
            CREATE TABLE IF NOT EXISTS polls (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                text TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS users (
                uid INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR REPLACE
                INTO users (uid, name, is_admin)
                VALUES (160453507, 'Dmitry Bogdanov', 1);
            CREATE TABLE IF NOT EXISTS own_votes (
                poll_id INTEGER NOT NULL REFERENCES polls (id),
                uid INTEGER NOT NULL REFERENCES users (uid),
                vote_type INTEGER NOT NULL,
                t TEXT,
                PRIMARY KEY (poll_id, uid)
            );
            CREATE TABLE IF NOT EXISTS others_votes (
                poll_id INTEGER NOT NULL REFERENCES polls (id),
                uid INTEGER NOT NULL REFERENCES users (uid),
                vote_type INTEGER NOT NULL,
                t TEXT
            );
            CREATE TABLE IF NOT EXISTS messages (
                poll_id INTEGER NOT NULL REFERENCES polls (id),
                chat_id INTEGER,
                msg_id INTEGER,
                inline_message_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER NOT NULL,
                place TEXT,
                d TIMESTAMP,
                t TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS places (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                place TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS update_own_votes_datetime
                AFTER UPDATE ON own_votes
                BEGIN
                    UPDATE own_votes SET t = strftime('%Y-%m-%d %H:%M:%f', 'now')
                    WHERE rowid = NEW.rowid and OLD.vote_type != NEW.vote_type;
                END;
            CREATE TRIGGER IF NOT EXISTS update_others_votes_datetime
                AFTER UPDATE ON others_votes
                BEGIN
                    UPDATE others_votes SET t = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE rowid = NEW.rowid;
                END;
            CREATE TRIGGER IF NOT EXISTS insert_own_votes_datetime
                AFTER INSERT ON own_votes
                BEGIN
                    UPDATE own_votes SET t = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE rowid = NEW.rowid;
                END;
            CREATE TRIGGER IF NOT EXISTS insert_others_votes_datetime
                AFTER INSERT ON others_votes
                BEGIN
                    UPDATE others_votes SET t = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE rowid = NEW.rowid;
                END;
            COMMIT TRANSACTION;
            ''')

    def insert_poll(self, poll: Poll) -> PollId:
        log.info(f'Saving poll {poll}')
        with self._lock, self._db as db:
            cursor = db.cursor()
            cursor.execute('''
                INSERT INTO polls (text) values (:text);
            ''', {'text': poll.text})
            return cursor.lastrowid

    def select_poll(self, poll_id: PollId) -> PollExt:
        log.info(f'Selecting poll with id {poll_id}')
        cursor = self._db.cursor()
        cursor.execute('SELECT text FROM polls WHERE id = :poll_id', {'poll_id': poll_id})
        text = cursor.fetchone()[0]
        votes = self.select_votes(poll_id)
        votes_pro = list(filter(lambda x: x.is_pro(), votes))
        votes_cons = list(filter(lambda x: not x.is_pro(), votes))
        return PollExt(poll_id=poll_id, votes_pro=votes_pro, votes_cons=votes_cons, text=text)

    def insert_message(self, poll: PollId, message_id: MessageId):
        log.info(f'saving message {message_id} for poll {poll}')
        with self._lock, self._db as db:
            cursor = db.cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO messages(poll_id, chat_id, msg_id, inline_message_id) VALUES (?, ?, ?, ?)',
                [poll, message_id.chat_id, message_id.message_id, message_id.inline_message_id])

    def select_messages(self, poll_id: PollId) -> List[MessageId]:
        log.info(f'selecting messages for poll with id {poll_id}')
        c = self._db.cursor()
        c.execute('SELECT chat_id, msg_id, inline_message_id FROM messages WHERE poll_id = :poll_id',
                  {'poll_id': poll_id})
        return list(map(lambda x: MessageId(chat_id=x[0], message_id=x[1], inline_message_id=x[2]), c.fetchall()))

    def save_user(self, uid: int, name: str):
        log.info(f'saving user {name} (uid={uid})')
        with self._lock, self._db as db:
            cursor = db.cursor()
            cursor.execute('INSERT OR REPLACE INTO users (uid, name) VALUES (:uid, :name)', {
                'uid': uid,
                'name': name
            })

    def start_sessions(self, user: User):
        with self._lock, self._db as db:
            cursor = db.cursor()
            cursor.execute('INSERT OR REPLACE INTO sessions (id) VALUES (:uid)', {'uid': user.id})

    def set_place_in_session(self, user: User, place: str):
        with self._lock, self._db as db:
            cursor = db.cursor()
            cursor.execute('UPDATE sessions SET place = :place WHERE id = :uid', {
                'place': place,
                'uid': user.id
            })

    def set_date_in_session(self, user: User, date: datetime.date):
        fake_time = datetime.datetime.now().time()
        with self._lock, self._db as db:
            cursor = db.cursor()
            cursor.execute('UPDATE sessions SET d = :date WHERE id = :uid', {
                'date': datetime.datetime.combine(date, fake_time),
                'uid': user.id
            })

    def set_time_in_session(self, user: User, time: datetime.time):
        fake_date = datetime.datetime.now().date()
        with self._lock, self._db as db:
            cursor = db.cursor()
            cursor.execute('UPDATE sessions SET t = :time WHERE id = :uid', {
                'time': datetime.datetime.combine(fake_date, time),
                'uid': user.id
            })

    def get_session(self, user: User) -> Tuple[str, datetime.date, datetime.time]:
        c = self._db.cursor().execute('SELECT place, d, t FROM sessions where id = :uid', {'uid': user.id})
        r = c.fetchone()
        return r[0], r[1].date(), r[2].time()

    def insert_place(self, place: str):
        with self._lock, self._db as db:  # type: sqlite3.Connection
            cursor = db.cursor()
            cursor.execute('INSERT OR IGNORE INTO places (place) VALUES (?)', [place])

    def select_places(self) -> Dict[int, str]:
        c = self._db.cursor()
        c.execute('SELECT id, place FROM places')
        places = {p[0]: p[1] for p in c.fetchall()}
        return places

    def remove_place(self, id: int):
        with self._db as db:
            cursor = db.cursor()
            cursor.execute('DELETE FROM places WHERE id = :id', {'id': id})

    def vote(self, poll_id: PollId, user: User, opt: OPTION):
        uid = user.id
        with self._lock, self._db as db:  # type: sqlite3.Connection
            cursor = db.cursor()
            cursor.execute('BEGIN TRANSACTION;')
            if opt == OPTION.ME_TOO:
                self.__insert_own_vote_in_tx(cursor, poll_id, uid, VoteType.PRO)
            elif opt == OPTION.ME_NOT:
                self.__insert_own_vote_in_tx(cursor, poll_id, uid, VoteType.CONS)
            elif opt == OPTION.PLUS_ONE:
                cursor.execute('''
                    INSERT INTO others_votes (poll_id, uid, vote_type)
                    VALUES (:poll_id, :uid, :vote_type);
                    ''', {'poll_id': poll_id, 'uid': uid, 'vote_type': VoteType.PLUS_ONE})
            elif opt == OPTION.MINUS_ONE:
                cursor.execute('''
                    DELETE FROM others_votes
                    WHERE rowid = (SELECT MAX(rowid) FROM others_votes WHERE poll_id = :poll_id AND uid = :uid)
                ''', {'poll_id': poll_id, 'uid': uid})
            else:
                log.error(f'Unknown option: {opt}. poll: {poll_id}, user: {user}', opt, poll_id, user)
                raise ValueError('Unknown option')

    def _execute(self, request: str, **kwargs):
        self._db.execute(request, kwargs)

    @staticmethod
    def __insert_own_vote_in_tx(cursor: sqlite3.Cursor, poll_id: PollId, uid: int, vote_type: VoteType):
        cursor.execute('''
            INSERT OR IGNORE INTO own_votes (poll_id, uid, vote_type)
            VALUES (:poll_id, :uid, :vote_type)
            ''', {'poll_id': poll_id, 'uid': uid, 'vote_type': vote_type})
        cursor.execute('''
            UPDATE own_votes
            SET vote_type = :vote_type
            WHERE poll_id = :poll_id AND uid = :uid
            ''', {'poll_id': poll_id, 'uid': uid, 'vote_type': vote_type})

    def select_votes(self, poll_id: PollId) -> List[Vote]:
        c = self._db.cursor()
        c.execute('''
            SELECT vote_type, uid, name FROM (
                SELECT vote_type, users.uid as uid, name, t
                    FROM own_votes
                    JOIN users ON own_votes.uid = users.uid
                    WHERE poll_id = :poll_id
                UNION ALL
                SELECT vote_type, users.uid as uid, name, t
                    FROM others_votes
                    JOIN users ON others_votes.uid = users.uid
                    WHERE poll_id = :poll_id
            ) ORDER BY t
        ''', {'poll_id': poll_id})
        votes = []
        for row in c.fetchall():
            vote_type = row[0]
            uid = row[1]
            name = row[2]
            votes.append(Vote(uid=uid, name=name, vote_type=vote_type))
        return votes

    def upsert_user(self, user: User):
        log.info(f'updating user uid={user.id}, name={user.name}')
        with self._lock, self._db as db:
            db.execute('''
            INSERT INTO users (uid, name)
            VALUES (:uid, :name)
            ON CONFLICT(uid) DO
                UPDATE
                SET name = :name
                WHERE uid = :uid
            ''', {'uid': user.id, 'name': user.full_name})
