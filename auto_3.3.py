#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import asyncio
import aiohttp
import aiofiles
import re
import time
import random
import hashlib
import numpy as np
import csv
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple, Set
from collections import deque, Counter
import logging
import pickle
from dataclasses import dataclass, field, asdict
import traceback
import signal

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters, ConversationHandler
)
from telegram.error import BadRequest
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError, PhoneCodeExpiredError


# ==================== 配置 ====================
class Config:
    BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
    API_ID = int(os.environ.get('API_ID', ))
    API_HASH = os.environ.get('API_HASH', '')

    PC28_API_BASE = "https://www.pc28.help/api/kj.json?nbr=500"
    ADMIN_USER_IDS = [5338954122]

    DATA_DIR = Path("data")
    SESSIONS_DIR = DATA_DIR / "sessions"
    LOGS_DIR = DATA_DIR / "logs"
    CACHE_DIR = DATA_DIR / "cache"
    INITIAL_HISTORY_SIZE = 100
    CACHE_SIZE = 200
    DEFAULT_BASE_AMOUNT = 20000
    DEFAULT_MAX_AMOUNT = 1000000
    DEFAULT_MULTIPLIER = 2.0
    DEFAULT_STOP_LOSS = 0
    DEFAULT_STOP_WIN = 0
    DEFAULT_STOP_BALANCE = 0
    DEFAULT_RESUME_BALANCE = 0
    MIN_BET_AMOUNT = 1
    MAX_BET_AMOUNT = 10000000
    EXCHANGE_RATE = 100000
    BALANCE_BOT = "kkpayPc28Bot"
    REQUEST_TIMEOUT = 15
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2
    MAX_HISTORY = 61
    GAME_CYCLE_SECONDS = 210
    CLOSE_BEFORE_SECONDS = 50
    MANUAL_LINK = "https://t.me/yugejnd/9"

    SCHEDULER_CHECK_INTERVAL = 5
    HEALTH_CHECK_INTERVAL = 60

    EXPLORATION_RATE = 0.10
    EXPLORATION_MIN = 0.01
    EXPLORATION_DECAY = 0.99
    NOISE_SCALE = 0.05

    MODEL_SAVE_FILE = "pc28_model.json"
    PATTERNS_FILE = "pc28_patterns.json"
    LONG_TERM_MEMORY_FILE = "pc28_memory.json"
    TRAINING_STATE_FILE = "training_state.json"

    BALANCE_CACHE_SECONDS = 30
    MAX_CONCURRENT_BETS = 5
    LOG_RETENTION_DAYS = 7
    ACCOUNT_SAVE_INTERVAL = 30
    MAX_CONCURRENT_PREDICTIONS = 3

    LOGIN_SELECT, LOGIN_CODE, LOGIN_PASSWORD = range(3)
    ADD_ACCOUNT = 10
    CHASE_NUMBERS, CHASE_PERIODS, CHASE_AMOUNT = range(11, 14)

    MAX_ACCOUNTS_PER_USER = 5

    KENO_HISTORY_DOWNLOAD = 5000
    KJ_HISTORY_DOWNLOAD = 5000

    # 训练参数
    TRAIN_EPOCHS = 200
    TRAIN_BATCH_SIZE = 100
    TRAIN_VALIDATION_SPLIT = 0.2
    MIN_TRAIN_DATA = 1000
    TRAIN_SAMPLES_PER_EPOCH = 800
    TRAIN_VALIDATION_SAMPLES = 200
    TRAIN_PATIENCE = 10
    TRAIN_LR = 0.001
    TRAIN_LR_DECAY = 0.5
    TRAIN_COOLDOWN = 5

    # 播报历史记录条数
    PREDICTION_HISTORY_SIZE = 20

    # 风险系数（用于一键推荐模式）
    RISK_PROFILES = {
        '保守': 0.005,   # 0.5%
        '稳定': 0.01,    # 1%
        '激进': 0.02,    # 2%
    }

    # 一键推荐金额参数（保留，但不再直接使用）
    RECOMMEND_BASE_RISK = 0.01
    RECOMMEND_RISK_RANGE = 0.02

    @classmethod
    def init_dirs(cls):
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.SESSIONS_DIR.mkdir(exist_ok=True)
        cls.LOGS_DIR.mkdir(exist_ok=True)
        cls.CACHE_DIR.mkdir(exist_ok=True)

    @classmethod
    def validate(cls):
        errors = []
        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN未配置")
        if cls.API_ID <= 0:
            errors.append("API_ID必须为正整数")
        if not cls.API_HASH:
            errors.append("API_HASH未配置")
        if not cls.PC28_API_BASE.startswith(('http://', 'https://')):
            errors.append("PC28_API_BASE必须是有效的URL")
        if cls.MIN_BET_AMOUNT < 0:
            errors.append("最小投注金额不能为负数")
        if cls.MAX_BET_AMOUNT <= cls.MIN_BET_AMOUNT:
            errors.append("最大投注金额必须大于最小投注金额")
        if cls.MAX_CONCURRENT_BETS < 1:
            errors.append("并发投注数至少为1")
        if errors:
            raise ValueError("配置验证失败: " + ", ".join(errors))
        return True

Config.init_dirs()

# ==================== 工具函数 ====================
def increment_qihao(current_qihao: str) -> str:
    if not current_qihao:
        return "1"
    match = re.search(r'(\d+)$', current_qihao)
    if match:
        num_part = match.group(1)
        prefix = current_qihao[:match.start()]
        try:
            next_num = str(int(num_part) + 1).zfill(len(num_part))
            return prefix + next_num
        except:
            return current_qihao + "1"
    else:
        try:
            return str(int(current_qihao) + 1)
        except:
            return current_qihao + "1"

# ==================== 彩色日志 ====================
class ColoredFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    red = "\x1b[31;20m"
    yellow = "\x1b[33;20m"
    blue = "\x1b[34;20m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.INFO: grey + "%(asctime)s [%(levelname)s] %(message)s" + reset,
        logging.ERROR: red + "%(asctime)s [%(levelname)s] %(message)s" + reset,
        'BETTING': green + "%(asctime)s [投注] %(message)s" + reset,
        'PREDICTION': blue + "%(asctime)s [预测] %(message)s" + reset,
    }

    def format(self, record):
        if hasattr(record, 'betting') and record.betting:
            self._style._fmt = self.FORMATS['BETTING']
        elif hasattr(record, 'prediction') and record.prediction:
            self._style._fmt = self.FORMATS['PREDICTION']
        else:
            self._style._fmt = self.FORMATS.get(record.levelno, self.grey + "%(asctime)s [%(levelname)s] %(message)s" + self.reset)
        return super().format(record)

class BotLogger:
    def __init__(self):
        self.logger = logging.getLogger('PC28Bot')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(ColoredFormatter(datefmt='%H:%M:%S'))
        self.logger.addHandler(console)

        log_file = Config.LOGS_DIR / f"bot_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        self.logger.addHandler(file_handler)

        self._clean_old_logs()

    def _clean_old_logs(self):
        now = datetime.now()
        for f in Config.LOGS_DIR.glob("bot_*.log"):
            try:
                date_str = f.stem.split('_')[1]
                file_date = datetime.strptime(date_str, '%Y%m%d')
                if (now - file_date).days > Config.LOG_RETENTION_DAYS:
                    f.unlink()
            except:
                pass

    def log_system(self, msg): self.logger.info(f"[系统] {msg}")
    def log_account(self, user_id, phone, action): self.logger.info(f"[账户] 用户:{user_id} 手机:{self._mask_phone(phone)} {action}")
    def log_game(self, msg): self.logger.info(f"[游戏] {msg}")
    def log_betting(self, user_id, action, detail):
        extra = {'betting': True}
        self.logger.info(f"用户:{user_id} {action} {detail}", extra=extra)
    def log_prediction(self, user_id, action, detail):
        extra = {'prediction': True}
        self.logger.info(f"用户:{user_id} {action} {detail}", extra=extra)
    def log_analysis(self, msg): self.logger.debug(f"[分析] {msg}")
    def log_error(self, user_id, action, error):
        error_trace = traceback.format_exc()
        self.logger.error(f"[错误] 用户:{user_id} {action}: {error}\n{error_trace}")
    def log_api(self, action, detail): self.logger.debug(f"[API] {action} {detail}")

    def _mask_phone(self, phone: str) -> str:
        if len(phone) >= 8:
            return phone[:5] + "****" + phone[-3:]
        return phone

logger = BotLogger()

# ==================== 基础数据 ====================
COMBOS = ["小单", "小双", "大单", "大双"]
BASE_PROB = {"小单": 27.11, "小双": 23.83, "大单": 22.32, "大双": 26.74}
SUM_TO_COMBO = {
    0: "小双", 1: "小单", 2: "小双", 3: "小单", 4: "小双", 5: "小单", 6: "小双",
    7: "小单", 8: "小双", 9: "小单", 10: "小双", 11: "小单", 12: "小双", 13: "小单",
    14: "大双", 15: "大单", 16: "大双", 17: "大单", 18: "大双", 19: "大单", 20: "大双",
    21: "大单", 22: "大双", 23: "大单", 24: "大双", 25: "大单", 26: "大双", 27: "大单"
}
TRANSITION_MATRIX = {
    "小单": {"小单": 26.3, "小双": 23.9, "大单": 22.9, "大双": 26.9},
    "小双": {"小单": 27.2, "小双": 22.7, "大单": 22.9, "大双": 27.3},
    "大单": {"小单": 28.2, "小双": 23.9, "大单": 21.5, "大双": 26.5},
    "大双": {"小单": 27.0, "小双": 24.7, "大单": 21.9, "大双": 26.4}
}

# ==================== 预测算法（返回分数） ====================
class Algorithms:
    @staticmethod
    def prob_dist(history, latest) -> Dict[str, float]:
        if len(history) < 20:
            return {c: random.uniform(20, 30) for c in COMBOS}
        recent = list(history)[:20]
        freq = {c: sum(1 for h in recent if h['combo'] == c) for c in COMBOS}
        scores = {}
        for combo in COMBOS:
            base = BASE_PROB[combo]
            recent_pct = freq[combo] / 20 * 100
            adjusted = base + (base - recent_pct) * 0.2
            noise = random.uniform(1 - Config.NOISE_SCALE, 1 + Config.NOISE_SCALE)
            scores[combo] = adjusted * noise
        return scores

    @staticmethod
    def trend(history, latest) -> Dict[str, float]:
        if len(history) < 5:
            return {c: random.uniform(20, 30) for c in COMBOS}
        h = list(history)[:3]
        scores = {c: 50 for c in COMBOS}
        if len(set([x['combo'] for x in h])) == 1 and h[0]['combo'] == h[1]['combo'] == h[2]['combo']:
            scores[h[0]['combo']] += 30
            opposite = {"小单":"大双", "小双":"大单", "大单":"小双", "大双":"小单"}
            scores[opposite[h[0]['combo']]] += 20
        else:
            scores[latest['combo']] += 20
            opposite = {"小单":"大双", "小双":"大单", "大单":"小双", "大双":"小单"}
            scores[opposite[latest['combo']]] += 10
        return scores

    @staticmethod
    def sum_analysis(history, latest) -> Dict[str, float]:
        if len(history) < 10:
            return {c: random.uniform(20, 30) for c in COMBOS}
        h = list(history)
        avg = sum(x['sum'] for x in h[:5]) / 5
        scores = {c: 30 for c in COMBOS}
        if avg > 16:
            for c in ["小单", "小双"]:
                scores[c] += 40
        elif avg < 11:
            for c in ["大单", "大双"]:
                scores[c] += 40
        else:
            last_parity = latest['sum'] % 2
            if last_parity:
                for c in ["大单", "小单"]:
                    scores[c] += 30
            else:
                for c in ["大双", "小双"]:
                    scores[c] += 30
        return scores

    @staticmethod
    def cold_hot(history, latest) -> Dict[str, float]:
        if len(history) < 30:
            return {c: random.uniform(20, 30) for c in COMBOS}
        h = list(history)[:30]
        freq = {c: sum(1 for x in h if x['combo'] == c) for c in COMBOS}
        total = sum(freq.values())
        scores = {c: (1 - freq[c]/total) * 100 for c in COMBOS}
        for c in COMBOS:
            scores[c] += (freq[c]/total) * 20
        return scores

    @staticmethod
    def transition(history, latest) -> Dict[str, float]:
        probs = TRANSITION_MATRIX.get(latest['combo'], TRANSITION_MATRIX["大单"])
        return probs

    @staticmethod
    def continuous(history, latest) -> Dict[str, float]:
        if len(history) < 3:
            return {c: random.uniform(20, 30) for c in COMBOS}
        h = list(history)[:3]
        last_three = [x['combo'] for x in h]
        scores = {c: 30 for c in COMBOS}
        if len(set(last_three)) == 1:
            scores[last_three[0]] += 40
            opposite = {"小单":"大双", "小双":"大单", "大单":"小双", "大双":"小单"}
            scores[opposite[last_three[0]]] += 30
        elif len(set(last_three)) == 3:
            candidates = [c for c in COMBOS if c not in last_three]
            for c in candidates:
                scores[c] += 35
        else:
            scores[latest['combo']] += 25
        return scores

    @staticmethod
    def equilibrium(history, latest) -> Dict[str, float]:
        if len(history) < 50:
            return {c: random.uniform(20, 30) for c in COMBOS}
        h = list(history)[:50]
        freq = {c: sum(1 for x in h if x['combo'] == c) for c in COMBOS}
        total = sum(freq.values())
        scores = {c: (1 - freq[c]/total) * 100 for c in COMBOS}
        return scores

    @staticmethod
    def comprehensive(history, latest) -> Dict[str, float]:
        if len(history) < 10:
            return {c: random.uniform(20, 30) for c in COMBOS}
        h = list(history)
        scores = {c: BASE_PROB[c] for c in COMBOS}
        for combo in COMBOS:
            recent_freq = sum(1 for x in h[:10] if x['combo'] == combo)
            scores[combo] += recent_freq * 2
            trans_prob = TRANSITION_MATRIX[latest['combo']][combo]
            scores[combo] += trans_prob * 0.5
            noise = random.uniform(0.9, 1.1)
            scores[combo] *= noise
        return scores

    @staticmethod
    def theoretical(history, latest) -> Dict[str, float]:
        """理论概率（固定值）"""
        return BASE_PROB.copy()

# ==================== 模式识别类 ====================
class PatternRecognizer:
    def __init__(self):
        self.patterns_file = Config.PATTERNS_FILE
        self.patterns = self.load_patterns()
        self.min_pattern_occurrences = 3

    def load_patterns(self):
        try:
            if os.path.exists(self.patterns_file):
                with open(self.patterns_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.log_error(0, "加载模式库失败", e)
        return {}

    def save_patterns(self):
        try:
            with open(self.patterns_file, 'w', encoding='utf-8') as f:
                json.dump(self.patterns, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.log_error(0, "保存模式库失败", e)

    def learn_pattern(self, history):
        if len(history) < 5:
            return
        recent = [h['combo'] for h in history[:5]]
        pattern_key = ','.join(recent[:-1])
        next_combo = recent[-1]
        if pattern_key not in self.patterns:
            self.patterns[pattern_key] = {}
        if next_combo not in self.patterns[pattern_key]:
            self.patterns[pattern_key][next_combo] = 0
        self.patterns[pattern_key][next_combo] += 1
        self.save_patterns()

    def match_pattern(self, history) -> Optional[Dict[str, float]]:
        if len(history) < 4:
            return None
        recent = [h['combo'] for h in history[:4]]
        pattern_key = ','.join(recent)
        if pattern_key in self.patterns:
            outcomes = self.patterns[pattern_key]
            total = sum(outcomes.values())
            if total >= self.min_pattern_occurrences:
                scores = {c: outcomes.get(c, 0) / total * 100 for c in COMBOS}
                return scores
        return None

# ==================== 趋势分析类 ====================
class TrendAnalyzer:
    def __init__(self):
        self.trend_history = deque(maxlen=100)

    def analyze_trend(self, history):
        if len(history) < 10:
            return {'type': 'unknown', 'confidence': 50}
        recent = list(history)[:20]
        combos = [h['combo'] for h in recent]
        streak = 1
        for i in range(1, len(combos)):
            if combos[i] == combos[i-1]:
                streak += 1
            else:
                break
        if streak >= 4:
            return {
                'type': 'strong_streak',
                'combo': combos[0],
                'streak': streak,
                'confidence': 70 + min(20, streak * 5)
            }
        sizes = [h['size'] for h in recent]
        big_count = sizes.count('大')
        small_count = sizes.count('小')
        if big_count >= 15:
            return {'type': 'strong_big', 'ratio': big_count/20, 'confidence': 70}
        elif small_count >= 15:
            return {'type': 'strong_small', 'ratio': small_count/20, 'confidence': 70}
        parities = [h['parity'] for h in recent]
        odd_count = parities.count('单')
        even_count = parities.count('双')
        if odd_count >= 15:
            return {'type': 'strong_odd', 'ratio': odd_count/20, 'confidence': 70}
        elif even_count >= 15:
            return {'type': 'strong_even', 'ratio': even_count/20, 'confidence': 70}
        return {'type': 'normal', 'confidence': 50}

# ==================== 长期记忆类 ====================
class LongTermMemory:
    def __init__(self):
        self.memory_file = Config.LONG_TERM_MEMORY_FILE
        self.memory = self.load_memory()

    def load_memory(self):
        try:
            if os.path.exists(self.memory_file):
                with open(self.memory_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.log_error(0, "加载长期记忆失败", e)
        return {
            'hourly_stats': {},
            'daily_stats': {},
            'position_stats': {},
            'accuracy_history': []
        }

    def save_memory(self):
        try:
            with open(self.memory_file, 'w', encoding='utf-8') as f:
                json.dump(self.memory, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.log_error(0, "保存长期记忆失败", e)

    def learn(self, history):
        for i, h in enumerate(history):
            hour = h.get('parsed_time', datetime.now()).hour
            if str(hour) not in self.memory['hourly_stats']:
                self.memory['hourly_stats'][str(hour)] = {c: 0 for c in COMBOS}
            self.memory['hourly_stats'][str(hour)][h['combo']] += 1
            qihao = h.get('qihao', '')
            if qihao and len(qihao) >= 2:
                position = qihao[-2:]
                if position not in self.memory['position_stats']:
                    self.memory['position_stats'][position] = {c: 0 for c in COMBOS}
                self.memory['position_stats'][position][h['combo']] += 1
        self.save_memory()

    def get_hourly_scores(self, hour) -> Optional[Dict[str, float]]:
        hour = str(hour)
        if hour in self.memory['hourly_stats']:
            stats = self.memory['hourly_stats'][hour]
            total = sum(stats.values())
            if total > 0:
                return {c: stats[c] / total * 100 for c in COMBOS}
        return None

    def get_position_scores(self, position) -> Optional[Dict[str, float]]:
        if position in self.memory['position_stats']:
            stats = self.memory['position_stats'][position]
            total = sum(stats.values())
            if total > 0:
                return {c: stats[c] / total * 100 for c in COMBOS}
        return None

# ==================== Keno相似性分析算法 ====================
class KenoSimilarity:
    def __init__(self):
        self.keno_history = deque(maxlen=5000)
        self.kj_history = deque(maxlen=5000)
        self.current_keno_nbrs = None
        self.model_file = Config.CACHE_DIR / "keno_model.pkl"

    def add_keno_data(self, keno_item, kj_item=None):
        self.keno_history.append(keno_item)
        if kj_item:
            self.kj_history.append(kj_item)

    def extract_features(self, nbrs):
        total = sum(nbrs)
        odd_cnt = sum(1 for n in nbrs if n % 2)
        even_cnt = 20 - odd_cnt
        small_cnt = sum(1 for n in nbrs if n <= 40)
        big_cnt = 20 - small_cnt
        bins = [0,0,0,0]
        for n in nbrs:
            if 1 <= n <= 20: bins[0] += 1
            elif 21 <= n <= 40: bins[1] += 1
            elif 41 <= n <= 60: bins[2] += 1
            else: bins[3] += 1

        primes = {2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79}
        prime_cnt = sum(1 for n in nbrs if n in primes)

        sorted_nbrs = sorted(nbrs)
        consecutive_pairs = 0
        for i in range(len(sorted_nbrs)-1):
            if sorted_nbrs[i+1] - sorted_nbrs[i] == 1:
                consecutive_pairs += 1

        if len(sorted_nbrs) > 1:
            gaps = [sorted_nbrs[i+1] - sorted_nbrs[i] for i in range(len(sorted_nbrs)-1)]
            avg_gap = sum(gaps) / len(gaps)
        else:
            avg_gap = 0

        return [total, odd_cnt, even_cnt, small_cnt, big_cnt] + bins + [prime_cnt, consecutive_pairs, avg_gap]

    def predict_scores(self, current_nbrs, top_k=30) -> Dict[str, float]:
        if len(self.keno_history) < 100:
            return {c: 25 for c in COMBOS}

        current_feat = self.extract_features(current_nbrs)
        similarities = []
        kenos = list(self.keno_history)
        total_history = len(kenos)

        for i, item in enumerate(kenos):
            if i == total_history - 1:
                continue
            feat = self.extract_features(item['nbrs'])
            dist = np.linalg.norm(np.array(current_feat) - np.array(feat))
            time_weight = np.exp(- (total_history - 1 - i) / 100)
            sim = 1.0 / (dist + 1e-6)
            weight = sim * time_weight
            similarities.append((weight, i, item))

        similarities.sort(key=lambda x: x[0], reverse=True)

        weighted_counts = Counter()
        total_weight = 0
        for weight, idx, item in similarities[:top_k]:
            if idx < len(self.kj_history):
                next_kj = self.kj_history[idx]
                if 'combo' in next_kj:
                    combo = next_kj['combo']
                    weighted_counts[combo] += weight
                    total_weight += weight

        if total_weight == 0:
            return {c: 25 for c in COMBOS}

        scores = {c: weighted_counts[c] / total_weight * 100 for c in COMBOS}
        return scores

    def save_model(self):
        try:
            model_data = {
                'keno_history': list(self.keno_history),
                'kj_history': list(self.kj_history)
            }
            with open(self.model_file, 'wb') as f:
                pickle.dump(model_data, f)
        except Exception as e:
            logger.log_error(0, "保存Keno模型失败", e)

    def load_model(self):
        try:
            if self.model_file.exists():
                with open(self.model_file, 'rb') as f:
                    model_data = pickle.load(f)
                self.keno_history.extend(model_data.get('keno_history', []))
                self.kj_history.extend(model_data.get('kj_history', []))
        except Exception as e:
            logger.log_error(0, "加载Keno模型失败", e)

# ==================== 强化学习模型 ====================
class RLModel:
    def __init__(self, learning_rate=0.1, discount_factor=0.9, exploration_rate=None):
        if exploration_rate is None:
            exploration_rate = Config.EXPLORATION_RATE
        self.q_table = {}
        self.lr = learning_rate
        self.gamma = discount_factor
        self.epsilon = exploration_rate
        self.last_state = None
        self.last_action = None
        self.combo_to_index = {combo: i for i, combo in enumerate(COMBOS)}
        self.index_to_combo = {i: combo for i, combo in enumerate(COMBOS)}
        self.model_file = Config.CACHE_DIR / "rl_model.json"
        self.training_history = deque(maxlen=1000)

    def _state_to_key(self, recent_combos):
        if len(recent_combos) < 3:
            return None
        key = 0
        for i, combo in enumerate(recent_combos[-3:]):
            key = key * 4 + self.combo_to_index[combo]
        return key

    def get_action_scores(self, state_key) -> Dict[str, float]:
        if state_key is None:
            return {c: 25 for c in COMBOS}
        if state_key not in self.q_table:
            self.q_table[state_key] = [25.0] * 4
        q_values = self.q_table[state_key]
        min_q = min(q_values)
        max_q = max(q_values)
        if max_q > min_q:
            scores = [(q - min_q) / (max_q - min_q) * 100 for q in q_values]
        else:
            scores = [50] * 4
        return {self.index_to_combo[i]: scores[i] for i in range(4)}

    def learn(self, state_key, action, reward, next_state_key):
        if state_key is None or next_state_key is None:
            return
        if state_key not in self.q_table:
            self.q_table[state_key] = [25.0] * 4
        if next_state_key not in self.q_table:
            self.q_table[next_state_key] = [25.0] * 4

        action_idx = self.combo_to_index[action]
        current_q = self.q_table[state_key][action_idx]
        max_next_q = max(self.q_table[next_state_key])
        new_q = current_q + self.lr * (reward + self.gamma * max_next_q - current_q)
        self.q_table[state_key][action_idx] = new_q

        self.training_history.append({
            'state': state_key,
            'action': action,
            'reward': reward,
            'next_state': next_state_key,
            'timestamp': datetime.now().isoformat()
        })

    def save(self):
        try:
            with open(self.model_file, 'w') as f:
                json.dump({str(k): v for k, v in self.q_table.items()}, f)
        except Exception as e:
            logger.log_error(0, "保存强化学习模型失败", e)

    def load(self):
        try:
            if self.model_file.exists():
                with open(self.model_file, 'r') as f:
                    data = json.load(f)
                    self.q_table = {int(k): v for k, v in data.items()}
        except Exception as e:
            logger.log_error(0, "加载强化学习模型失败", e)

# ==================== 训练状态管理 ====================
class TrainingState:
    def __init__(self):
        self.state_file = Config.CACHE_DIR / Config.TRAINING_STATE_FILE
        self.current_epoch = 0
        self.best_accuracy = 0
        self.best_weights = {}
        self.training_start_time = None
        self.last_qihao = None
        self.load()

    def load(self):
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    self.current_epoch = data.get('current_epoch', 0)
                    self.best_accuracy = data.get('best_accuracy', 0)
                    self.best_weights = data.get('best_weights', {})
                    self.training_start_time = data.get('training_start_time')
                    self.last_qihao = data.get('last_qihao')
        except Exception as e:
            logger.log_error(0, "加载训练状态失败", e)

    def save(self):
        try:
            data = {
                'current_epoch': self.current_epoch,
                'best_accuracy': self.best_accuracy,
                'best_weights': self.best_weights,
                'training_start_time': self.training_start_time,
                'last_qihao': self.last_qihao
            }
            with open(self.state_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.log_error(0, "保存训练状态失败", e)

    def reset(self):
        self.current_epoch = 0
        self.best_accuracy = 0
        self.best_weights = {}
        self.training_start_time = datetime.now().isoformat()
        self.save()

# ==================== 模型管理器（稳定融合版） ====================
class ModelManager:
    def __init__(self):
        # 所有权重初始为2.5，理论概率略高作为锚点
        self.weights = {
            "概率分布": 2.5,
            "趋势分析": 2.5,
            "和值分析": 2.5,
            "冷热分析": 2.5,
            "转移概率": 2.5,
            "连续模式": 2.5,
            "均衡回归": 2.5,
            "综合推荐": 2.5,
            "Keno相似性": 2.5,
            "强化学习": 2.5,
            "模式识别": 2.5,
            "小时规律": 2.5,
            "位置规律": 2.5,
            "趋势强化": 2.5,
            "理论概率": 3.0,      # 锚点算法
        }
        self.exploration_rate = Config.EXPLORATION_RATE
        self.prediction_history = []
        self.algos = [
            ("概率分布", Algorithms.prob_dist),
            ("趋势分析", Algorithms.trend),
            ("和值分析", Algorithms.sum_analysis),
            ("冷热分析", Algorithms.cold_hot),
            ("转移概率", Algorithms.transition),
            ("连续模式", Algorithms.continuous),
            ("均衡回归", Algorithms.equilibrium),
            ("综合推荐", Algorithms.comprehensive),
            ("Keno相似性", self._keno_scores),
            ("强化学习", self._rl_scores),
            ("模式识别", self._pattern_scores),
            ("小时规律", self._hour_scores),
            ("位置规律", self._position_scores),
            ("趋势强化", self._trend_scores),
            ("理论概率", Algorithms.theoretical),
        ]
        self.pattern_recognizer = PatternRecognizer()
        self.trend_analyzer = TrendAnalyzer()
        self.long_term_memory = LongTermMemory()
        self.keno_similarity = KenoSimilarity()
        self.rl_model = RLModel()
        self.algorithm_accuracy = {name: deque(maxlen=100) for name, _ in self.algos}
        self.recent_accuracy = deque(maxlen=50)
        self.current_keno_nbrs = None
        self.training_state = TrainingState()
        self.is_training = False
        self._save_lock = asyncio.Lock()
        self.load()
        self.rl_model.load()
        self.keno_similarity.load_model()

    def load(self):
        try:
            if os.path.exists(Config.MODEL_SAVE_FILE):
                with open(Config.MODEL_SAVE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    saved_weights = data.get('weights', {})
                    for name in self.weights:
                        if name in saved_weights:
                            self.weights[name] = max(1.0, min(5.0, saved_weights[name]))
                    self.exploration_rate = data.get('exploration_rate', Config.EXPLORATION_RATE)
        except Exception as e:
            logger.log_error(0, "加载模型权重失败", e)

    async def save(self):
        async with self._save_lock:
            try:
                data = {
                    'weights': self.weights,
                    'exploration_rate': self.exploration_rate,
                    'last_save': datetime.now().isoformat()
                }
                async with aiofiles.open(Config.MODEL_SAVE_FILE, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            except Exception as e:
                logger.log_error(0, "保存模型权重失败", e)

    def reset_weights(self):
        """重置所有权重为初始值，清空所有历史"""
        self.weights = {
            "概率分布": 2.5,
            "趋势分析": 2.5,
            "和值分析": 2.5,
            "冷热分析": 2.5,
            "转移概率": 2.5,
            "连续模式": 2.5,
            "均衡回归": 2.5,
            "综合推荐": 2.5,
            "Keno相似性": 2.5,
            "强化学习": 2.5,
            "模式识别": 2.5,
            "小时规律": 2.5,
            "位置规律": 2.5,
            "趋势强化": 2.5,
            "理论概率": 3.0,
        }
        self.exploration_rate = Config.EXPLORATION_RATE
        self.prediction_history.clear()
        for acc in self.algorithm_accuracy.values():
            acc.clear()
        self.recent_accuracy.clear()
        logger.log_system("模型权重已重置为初始值")

    def _keno_scores(self, history, latest) -> Dict[str, float]:
        if self.current_keno_nbrs:
            return self.keno_similarity.predict_scores(self.current_keno_nbrs)
        return {c: 25 for c in COMBOS}

    def _rl_scores(self, history, latest) -> Dict[str, float]:
        if len(history) < 3:
            return {c: 25 for c in COMBOS}
        recent_combos = [h['combo'] for h in history[:3]]
        state_key = self.rl_model._state_to_key(recent_combos)
        return self.rl_model.get_action_scores(state_key)

    def _pattern_scores(self, history, latest) -> Dict[str, float]:
        scores = self.pattern_recognizer.match_pattern(history)
        return scores if scores else {c: 25 for c in COMBOS}

    def _hour_scores(self, history, latest) -> Dict[str, float]:
        hour = datetime.now().hour
        scores = self.long_term_memory.get_hourly_scores(hour)
        return scores if scores else {c: 25 for c in COMBOS}

    def _position_scores(self, history, latest) -> Dict[str, float]:
        qihao = latest.get('qihao', '')
        if len(qihao) >= 2:
            position = qihao[-2:]
            scores = self.long_term_memory.get_position_scores(position)
            if scores:
                return scores
        return {c: 25 for c in COMBOS}

    def _trend_scores(self, history, latest) -> Dict[str, float]:
        trend = self.trend_analyzer.analyze_trend(history)
        if trend['type'] == 'normal':
            return {c: 25 for c in COMBOS}
        scores = {c: 0 for c in COMBOS}
        if trend['type'] == 'strong_streak':
            scores[trend['combo']] = trend['confidence']
        elif trend['type'] == 'strong_big':
            for c in ["大单", "大双"]:
                scores[c] = trend['confidence']
        elif trend['type'] == 'strong_small':
            for c in ["小单", "小双"]:
                scores[c] = trend['confidence']
        elif trend['type'] == 'strong_odd':
            for c in ["大单", "小单"]:
                scores[c] = trend['confidence']
        elif trend['type'] == 'strong_even':
            for c in ["大双", "小双"]:
                scores[c] = trend['confidence']
        total = sum(scores.values())
        if total > 0:
            for c in COMBOS:
                scores[c] = scores.get(c, 0) / total * 100
        return scores

    def predict(self, history, latest):
        """统一预测：主推、候选、杀组均来自同一套分数"""
        algo_scores_list = []
        for name, func in self.algos:
            try:
                scores = func(history, latest)
                algo_scores_list.append((name, scores))
            except Exception as e:
                logger.log_error(0, f"算法 {name} 执行异常", e)
                algo_scores_list.append((name, {c: 25 for c in COMBOS}))

        final_scores = {c: 0.0 for c in COMBOS}
        weight_sum = 0.0
        for name, scores in algo_scores_list:
            weight = self.weights.get(name, 2.5)
            for c in COMBOS:
                final_scores[c] += scores.get(c, 0) * weight
            weight_sum += weight

        if weight_sum > 0:
            for c in COMBOS:
                final_scores[c] /= weight_sum

        sorted_combos = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)
        main = sorted_combos[0][0]
        candidate = sorted_combos[1][0] if len(sorted_combos) > 1 else main
        kill = sorted_combos[-1][0]

        if random.random() < self.exploration_rate:
            other_combos = [c for c in COMBOS if c != main]
            main = random.choice(other_combos)
            if main == sorted_combos[0][0]:
                candidate = sorted_combos[1][0]
            else:
                candidate = sorted_combos[0][0]
            if main == sorted_combos[-1][0]:
                kill = sorted_combos[-2][0]
            else:
                kill = sorted_combos[-1][0]

        top_score = final_scores[main]
        second_score = final_scores[candidate] if candidate != main else final_scores[[c for c in COMBOS if c != main][0]]
        confidence = 50 + (top_score - second_score) / 2
        confidence = min(90, max(50, int(confidence)))

        result = {
            "main": main,
            "candidate": candidate,
            "kill": kill,
            "confidence": confidence,
            "scores": final_scores,
            "algo_details": [(name, scores) for name, scores in algo_scores_list],
        }
        return result

    async def learn(self, prediction, actual, qihao, sum_val):
        main = prediction['main']
        candidate = prediction['candidate']
        kill = prediction['kill']
        is_correct_double = (actual == main or actual == candidate)
        is_correct_kill = (actual != kill)

        record = {
            "time": datetime.now().isoformat(),
            "qihao": qihao,
            "main": main,
            "candidate": candidate,
            "kill": kill,
            "actual": actual,
            "sum": sum_val,
            "correct_double": is_correct_double,
            "correct_kill": is_correct_kill
        }
        self.prediction_history.append(record)
        self.recent_accuracy.append(1 if is_correct_double else 0)

        recent_correct = sum(self.recent_accuracy)
        recent_total = len(self.recent_accuracy)
        current_accuracy = recent_correct / recent_total if recent_total > 0 else 0.5

        base_lr = 0.05
        learning_rate = base_lr * (1 - current_accuracy)

        for name, scores in prediction.get('algo_details', []):
            actual_score = scores.get(actual, 0)
            other_scores = [scores[c] for c in COMBOS if c != actual]
            avg_other = sum(other_scores) / len(other_scores) if other_scores else 0

            if actual_score > avg_other:
                increase = learning_rate * (actual_score - avg_other) / 100
                self.weights[name] = min(5.0, self.weights.get(name, 2.5) + min(increase, 0.02))
            else:
                decrease = learning_rate * (avg_other - actual_score) / 100 * 0.5
                self.weights[name] = max(1.0, self.weights.get(name, 2.5) - min(decrease, 0.02))

            if name in self.algorithm_accuracy:
                self.algorithm_accuracy[name].append(1 if actual_score > avg_other else 0)

        if len(self.prediction_history) >= 2:
            prev_pred = self.prediction_history[-2]
            actual_combos = [p['actual'] for p in self.prediction_history[-4:-1] if p.get('actual')]
            if len(actual_combos) == 3 and prev_pred.get('actual') is not None:
                prev_state = self.rl_model._state_to_key(actual_combos)
                action = prev_pred['main']
                new_actual_combos = [p['actual'] for p in self.prediction_history[-3:] if p.get('actual')]
                if len(new_actual_combos) == 3:
                    next_state = self.rl_model._state_to_key(new_actual_combos)
                    if prev_pred['main'] == prev_pred['actual']:
                        reward = 1.0
                    elif prev_pred['candidate'] == prev_pred['actual']:
                        reward = 0.5
                    else:
                        reward = -0.5
                    self.rl_model.learn(prev_state, action, reward, next_state)

        self.exploration_rate = max(Config.EXPLORATION_MIN, self.exploration_rate * Config.EXPLORATION_DECAY)

        asyncio.create_task(self.save())
        if random.random() < 0.01:
            self.rl_model.save()

    def get_accuracy_stats(self):
        stats = {
            'overall': {
                'recent': sum(self.recent_accuracy) / len(self.recent_accuracy) if self.recent_accuracy else 0,
                'total': sum(1 for r in self.prediction_history if r.get('correct_double', False)) / len(self.prediction_history) if self.prediction_history else 0
            },
            'algorithms': {}
        }
        for name, acc_history in self.algorithm_accuracy.items():
            if acc_history:
                stats['algorithms'][name] = sum(acc_history) / len(acc_history)
        return stats

    def clear_history(self):
        self.prediction_history.clear()
        self.recent_accuracy.clear()
        for acc in self.algorithm_accuracy.values():
            acc.clear()
        self.long_term_memory = LongTermMemory()
        self.pattern_recognizer = PatternRecognizer()
        asyncio.create_task(self.save())

    async def fallback_kill(self, history):
        if len(history) < 10:
            return random.choice(COMBOS), 50
        combos = [h['combo'] for h in history[:50]]
        freq = Counter(combos)
        min_combo = min(freq, key=freq.get)
        confidence = 50 + min(40, (len(history) / 50) * 40)
        return min_combo, min(90, int(confidence))

    # ==================== 离线训练（使用验证集和早停） ====================
    async def offline_train(self, kj_data, keno_data, max_epochs=Config.TRAIN_EPOCHS):
        if self.is_training:
            logger.log_system("训练已在运行中")
            return

        if len(kj_data) < Config.MIN_TRAIN_DATA:
            logger.log_system(f"数据量不足 {Config.MIN_TRAIN_DATA} 条，跳过训练")
            return

        self.is_training = True
        logger.log_system("=" * 70)
        logger.log_system("🚀 开始离线训练（早停+学习率衰减）")
        logger.log_system(f"开奖数据: {len(kj_data)}条, Keno数据: {len(keno_data)}条")
        logger.log_system(f"最大训练轮数: {max_epochs}")
        logger.log_system("=" * 70)

        total = len(kj_data)
        train_end = int(total * 0.7)
        val_end = int(total * 0.85)
        train_data = kj_data[:train_end]
        val_data = kj_data[train_end:val_end]
        test_data = kj_data[val_end:]

        logger.log_system(f"训练集: {len(train_data)}条, 验证集: {len(val_data)}条, 测试集: {len(test_data)}条")

        patience = Config.TRAIN_PATIENCE
        best_accuracy = 0.0
        best_weights = self.weights.copy()
        no_improve_count = 0

        lr = Config.TRAIN_LR
        cooldown = Config.TRAIN_COOLDOWN
        cooldown_count = 0

        acc_history = deque(maxlen=patience)
        train_start = time.time()

        try:
            for epoch in range(max_epochs):
                epoch_start = time.time()

                await self._train_epoch_free(train_data, keno_data, epoch, lr)

                accuracy = await self._validate_fast(val_data)
                acc_history.append(accuracy)

                if accuracy > best_accuracy:
                    best_accuracy = accuracy
                    best_weights = self.weights.copy()
                    self.training_state.best_weights = best_weights
                    self.training_state.best_accuracy = best_accuracy
                    logger.log_system(f"  🏆 新最佳验证准确率: {accuracy:.2%}")
                    await self.save()
                    self.rl_model.save()
                    self.keno_similarity.save_model()
                    no_improve_count = 0
                    cooldown_count = 0
                else:
                    no_improve_count += 1
                    cooldown_count += 1

                if no_improve_count >= patience:
                    logger.log_system(f"  ⏹️ 早停触发，连续 {patience} 轮未提升")
                    break

                if cooldown_count >= cooldown:
                    lr *= Config.TRAIN_LR_DECAY
                    logger.log_system(f"  📉 学习率衰减至 {lr:.6f}")
                    cooldown_count = 0

                self.training_state.current_epoch = epoch + 1
                self.training_state.save()

                avg_acc = sum(acc_history) / len(acc_history) if acc_history else 0
                trend = "↑" if accuracy > avg_acc else "↓" if accuracy < avg_acc else "→"
                epoch_time = time.time() - epoch_start
                total_time = (time.time() - train_start) / 60
                logger.log_system(
                    f"Epoch {epoch+1}/{max_epochs} | "
                    f"验证准确率: {accuracy:.2%}{trend} | "
                    f"最佳: {best_accuracy:.2%} | "
                    f"未提升: {no_improve_count}/{patience} | "
                    f"学习率: {lr:.6f} | "
                    f"耗时: {epoch_time:.1f}s | 总: {total_time:.1f}min"
                )

                if (epoch + 1) % 5 == 0:
                    if await self._check_new_result():
                        logger.log_system("🎯 检测到新开奖，停止训练")
                        break

                await asyncio.sleep(2)

        except Exception as e:
            logger.log_error(0, "训练过程异常", e)
        finally:
            self.weights.update(best_weights)
            self.is_training = False
            if test_data:
                test_acc = await self._validate_fast(test_data)
                logger.log_system(f"📊 测试集最终准确率: {test_acc:.2%}")
            total_minutes = (time.time() - train_start) / 60
            logger.log_system("=" * 70)
            logger.log_system(f"✅ 训练结束 | 总耗时: {total_minutes:.1f}分钟 | 最佳验证准确率: {best_accuracy:.2%}")
            logger.log_system("=" * 70)

    async def _train_epoch_free(self, train_data, keno_data, epoch, lr):
        indices = list(range(len(train_data) - 50))
        random.shuffle(indices)

        max_samples = min(Config.TRAIN_SAMPLES_PER_EPOCH, len(indices))
        sample_indices = indices[:max_samples]

        correct_count = 0
        total = 0

        for idx in sample_indices:
            if idx + 50 >= len(train_data):
                continue

            history = train_data[idx:idx+50]
            latest = train_data[idx+49]
            actual = train_data[idx+50].get('combo')

            if not actual:
                continue

            for name, func in self.algos:
                try:
                    if name == "Keno相似性":
                        if keno_data and random.random() < 0.3:
                            keno_idx = random.randint(0, len(keno_data)-1)
                            self.current_keno_nbrs = keno_data[keno_idx].get('nbrs')
                            scores = func(history, latest)
                        else:
                            continue
                    else:
                        scores = func(history, latest)

                    if scores:
                        pred = max(scores, key=scores.get)
                        if pred == actual:
                            correct_count += 1
                            self.weights[name] = min(5.0, self.weights.get(name, 2.5) * (1 + lr * 0.1))
                        else:
                            self.weights[name] = max(1.0, self.weights.get(name, 2.5) * (1 - lr * 0.05))
                except Exception:
                    continue

            total += 1
            if total % 10 == 0:
                await asyncio.sleep(0)

        if total > 0:
            epoch_acc = correct_count / total
            logger.log_analysis(f"  训练集准确率: {epoch_acc:.2%} ({correct_count}/{total})")

    async def _validate_fast(self, val_data):
        if len(val_data) < 50:
            return 0

        random.seed(42)
        sample_size = min(Config.TRAIN_VALIDATION_SAMPLES, len(val_data) - 50)
        indices = random.sample(range(len(val_data) - 50), sample_size)

        correct = 0
        total = 0

        for idx in indices:
            history = val_data[idx:idx+50]
            latest = val_data[idx+49]
            actual = val_data[idx+50].get('combo')

            if not actual:
                continue

            final_scores = {c: 0 for c in COMBOS}
            weight_sum = 0

            for name, func in self.algos:
                try:
                    if name == "Keno相似性" and self.current_keno_nbrs:
                        scores = func(history, latest)
                    elif name != "Keno相似性":
                        scores = func(history, latest)
                    else:
                        continue

                    if scores:
                        weight = self.weights.get(name, 2.5)
                        for c in COMBOS:
                            final_scores[c] += scores.get(c, 0) * weight
                        weight_sum += weight
                except Exception:
                    continue

            if weight_sum > 0:
                for c in COMBOS:
                    final_scores[c] /= weight_sum
                pred = max(final_scores, key=final_scores.get)
                if pred == actual:
                    correct += 1
                total += 1

            await asyncio.sleep(0)

        return correct / total if total > 0 else 0

    async def _check_new_result(self):
        # 实际由GlobalScheduler覆盖
        return False

# ==================== API模块 ====================
class PC28API:
    def __init__(self):
        self.base_url = Config.PC28_API_BASE
        self.session = None
        self.call_stats = {
            'total_calls': 0, 'successful_calls': 0, 'failed_calls': 0,
            'last_call_time': None, 'last_success_time': None,
            'response_times': deque(maxlen=100)
        }
        self.cache_file = Config.CACHE_DIR / "history_cache.pkl"
        self.keno_cache_file = Config.CACHE_DIR / "keno_cache.pkl"
        self.history_cache = deque(maxlen=Config.CACHE_SIZE)
        self.keno_cache = deque(maxlen=5000)
        self.load_cache()
        logger.log_system("异步API模块初始化完成")

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT)
            )

    def load_cache(self):
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'rb') as f:
                    cache_data = pickle.load(f)
                self.history_cache.extend(cache_data[:Config.CACHE_SIZE])
            if self.keno_cache_file.exists():
                with open(self.keno_cache_file, 'rb') as f:
                    keno_data = pickle.load(f)
                self.keno_cache.extend(keno_data[:5000])
        except Exception as e:
            logger.log_error(0, "加载缓存失败", e)

    def save_cache(self):
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(list(self.history_cache), f)
            with open(self.keno_cache_file, 'wb') as f:
                pickle.dump(list(self.keno_cache), f)
        except Exception as e:
            logger.log_error(0, "保存缓存失败", e)

    async def _make_api_call(self, endpoint, params=None):
        await self.ensure_session()
        for retry in range(Config.MAX_RETRIES):
            self.call_stats['total_calls'] += 1
            start = time.time()
            try:
                url = f"{self.base_url}/{endpoint}.json"
                if params:
                    query_string = "&".join(f"{k}={v}" for k, v in params.items())
                    url = f"{url}?{query_string}"
                logger.log_api("请求", f"GET {url}")
                async with self.session.get(url) as resp:
                    resp.raise_for_status()
                    try:
                        data = await resp.json()
                    except json.JSONDecodeError as e:
                        logger.log_error(0, f"JSON解析失败 {endpoint}", e)
                        text = await resp.text()
                        logger.log_api("原始响应", text[:200])
                        if retry < Config.MAX_RETRIES - 1:
                            wait = Config.RETRY_BACKOFF ** retry
                            logger.log_api("重试", f"{retry+1}/{Config.MAX_RETRIES}, 等待{wait}秒")
                            await asyncio.sleep(wait)
                            continue
                        else:
                            self.call_stats['failed_calls'] += 1
                            return None
                    if data.get('message') != 'success':
                        logger.log_api("错误", f"API返回非success状态: {data}")
                        self.call_stats['failed_calls'] += 1
                        if retry < Config.MAX_RETRIES - 1:
                            wait = Config.RETRY_BACKOFF ** retry
                            logger.log_api("重试", f"{retry+1}/{Config.MAX_RETRIES}, 等待{wait}秒")
                            await asyncio.sleep(wait)
                            continue
                        else:
                            return None
                    elapsed = time.time() - start
                    self.call_stats['successful_calls'] += 1
                    self.call_stats['response_times'].append(elapsed)
                    self.call_stats['last_call_time'] = datetime.now()
                    self.call_stats['last_success_time'] = datetime.now()
                    logger.log_api("调用成功", f"{endpoint} 耗时 {elapsed:.2f}秒")
                    return data.get('data', [])
            except asyncio.TimeoutError:
                logger.log_api("超时", f"{endpoint}")
                if retry < Config.MAX_RETRIES - 1:
                    wait = Config.RETRY_BACKOFF ** retry
                    logger.log_api("重试", f"{retry+1}/{Config.MAX_RETRIES}, 等待{wait}秒")
                    await asyncio.sleep(wait)
                else:
                    self.call_stats['failed_calls'] += 1
                    logger.log_error(0, f"API调用最终超时 {endpoint}", None)
                    return None
            except aiohttp.ClientError as e:
                logger.log_api("请求异常", f"{endpoint}: {str(e)}")
                if retry < Config.MAX_RETRIES - 1:
                    wait = Config.RETRY_BACKOFF ** retry
                    logger.log_api("重试", f"{retry+1}/{Config.MAX_RETRIES}, 等待{wait}秒")
                    await asyncio.sleep(wait)
                else:
                    self.call_stats['failed_calls'] += 1
                    logger.log_error(0, f"API调用最终失败 {endpoint}", e)
                    return None
            except Exception as e:
                logger.log_error(0, f"API调用异常 {endpoint}", e)
                if retry < Config.MAX_RETRIES - 1:
                    await asyncio.sleep(Config.RETRY_BACKOFF ** retry)
                else:
                    self.call_stats['failed_calls'] += 1
                    return None
        return None

    async def download_csv_data(self, url: str) -> List[Dict]:
        await self.ensure_session()
        try:
            async with self.session.get(url) as resp:
                resp.raise_for_status()
                text = await resp.text()
                if text.startswith('\ufeff'):
                    text = text[1:]
                reader = csv.DictReader(StringIO(text))
                rows = []
                for row in reader:
                    clean_row = {k.strip(): v.strip() for k, v in row.items()}
                    rows.append(clean_row)
                logger.log_api("CSV下载", f"从 {url} 获取了 {len(rows)} 行")
                return rows
        except Exception as e:
            logger.log_error(0, f"下载CSV失败 {url}", e)
            return []

    def _parse_kj_csv_row(self, row: Dict) -> Optional[Dict]:
        try:
            qihao = row.get('期号', '').strip()
            date_str = row.get('日期', '').strip()
            time_str = row.get('时间', '').strip()
            number_str = row.get('号码', '').strip()
            combo = row.get('组合类型', '').strip()

            total = None
            if '+' in number_str:
                parts = number_str.split('+')
                if len(parts) == 3:
                    total = sum(int(p) for p in parts)

            if combo and len(combo) >= 2:
                size = combo[0]
                parity = combo[1]
            elif total is not None:
                size = "大" if total >= 14 else "小"
                parity = "单" if total % 2 else "双"
                combo = size + parity
            else:
                return None

            return {
                'qihao': qihao,
                'opentime': f"{date_str} {time_str}",
                'opennum': str(total) if total else '',
                'sum': total,
                'size': size,
                'parity': parity,
                'combo': combo,
                'parsed_time': self._parse_time(date_str, time_str),
                'fetch_time': datetime.now().isoformat(),
                'hash': hashlib.md5(f"{qihao}_{total}".encode()).hexdigest()[:8]
            }
        except Exception as e:
            logger.log_error(0, "解析开奖CSV行失败", e)
            return None

    def _parse_keno_csv_row(self, row: Dict) -> Optional[Dict]:
        try:
            qihao = row.get('期号', '').strip()
            date = row.get('日期', '').strip()
            time_str = row.get('时间', '').strip()
            nbrs_str = row.get('开奖号码', '').strip()
            bonus = row.get('奖金', '').strip()

            nbrs_str = nbrs_str.strip('"')
            nbrs = [int(x.strip()) for x in nbrs_str.split(',')]
            if len(nbrs) != 20:
                return None

            return {
                'qihao': qihao,
                'nbrs': nbrs,
                'date': date,
                'time': time_str,
                'bonus': bonus,
                'parsed_time': self._parse_time(date, time_str),
            }
        except Exception as e:
            logger.log_error(0, "解析Keno CSV行失败", e)
            return None

    async def fetch_kj(self, nbr=1):
        data = await self._make_api_call('kj', {'nbr': nbr})
        if not data:
            logger.log_api("fetch_kj", "返回空数据")
            return []
        processed = []
        for item in data:
            try:
                qihao = str(item.get('nbr', '')).strip()
                if not qihao:
                    logger.log_api("警告", "缺少期号字段")
                    continue
                number = item.get('number')
                if number is None:
                    number = item.get('num')
                if number is None:
                    logger.log_api("警告", f"期号{qihao}缺少number/num字段")
                    continue
                if isinstance(number, str) and '+' in number:
                    parts = number.split('+')
                    if len(parts) == 3:
                        try:
                            total = sum(int(p) for p in parts)
                        except ValueError:
                            logger.log_api("警告", f"期号{qihao} number格式错误: {number}")
                            continue
                    else:
                        logger.log_api("警告", f"期号{qihao} number格式异常: {number}")
                        continue
                else:
                    try:
                        total = int(number)
                    except (ValueError, TypeError):
                        logger.log_api("警告", f"期号{qihao} number不是整数: {number}")
                        continue
                combo = item.get('combination', '')
                if combo and len(combo) >= 2:
                    size = combo[0]
                    parity = combo[1]
                else:
                    size = "大" if total >= 14 else "小"
                    parity = "单" if total % 2 else "双"
                    combo = size + parity
                date_str = item.get('date', '')
                time_str = item.get('time', '')
                open_time = f"{date_str} {time_str}".strip()
                processed.append({
                    'qihao': qihao,
                    'opentime': open_time,
                    'opennum': str(total),
                    'sum': total,
                    'size': size,
                    'parity': parity,
                    'combo': combo,
                    'parsed_time': self._parse_time(date_str, time_str),
                    'fetch_time': datetime.now().isoformat(),
                    'hash': hashlib.md5(f"{qihao}_{total}".encode()).hexdigest()[:8]
                })
            except Exception as e:
                logger.log_error(0, f"处理开奖数据项失败", e)
                continue
        processed.sort(key=lambda x: x.get('parsed_time', datetime.now()), reverse=True)
        logger.log_api("fetch_kj", f"获取到 {len(processed)} 条有效数据")
        return processed

    async def fetch_keno(self, nbr=1):
        data = await self._make_api_call('keno', {'nbr': nbr})
        if not data:
            return []
        processed = []
        for item in data:
            try:
                qihao = str(item.get('nbr', ''))
                if not qihao:
                    logger.log_api("警告", "Keno数据缺少期号")
                    continue
                nbrs_str = item.get('nbrs', '')
                if not nbrs_str:
                    logger.log_api("警告", f"期号{qihao}缺少nbrs字段")
                    continue
                nbrs = []
                for x in nbrs_str.split(','):
                    x = x.strip()
                    if x and x.isdigit():
                        nbrs.append(int(x))
                if len(nbrs) != 20:
                    logger.log_api("警告", f"期号{qihao}的号码数量不正确: {len(nbrs)}")
                    continue
                date_str = item.get('date', '')
                time_str = item.get('time', '')
                processed.append({
                    'qihao': qihao,
                    'nbrs': nbrs,
                    'date': date_str,
                    'time': time_str,
                    'bonus': item.get('bonus', ''),
                    'parsed_time': self._parse_time(date_str, time_str),
                })
            except Exception as e:
                logger.log_error(0, f"处理Keno数据失败", e)
                continue
        processed.sort(key=lambda x: x.get('parsed_time', datetime.now()), reverse=True)
        logger.log_api("fetch_keno", f"获取到 {len(processed)} 条有效Keno数据")
        return processed

    def _parse_time(self, date_str, time_str):
        try:
            dt_str = f"{date_str} {time_str}".strip()
            if not dt_str or dt_str == ' ':
                return datetime.now()
            formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%m-%d %H:%M:%S", "%H:%M:%S"]
            for fmt in formats:
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    if fmt == "%H:%M:%S":
                        now = datetime.now()
                        dt = dt.replace(year=now.year, month=now.month, day=now.day)
                    elif fmt == "%m-%d %H:%M:%S":
                        dt = dt.replace(year=datetime.now().year)
                    return dt
                except ValueError:
                    continue
            return datetime.now()
        except Exception:
            return datetime.now()

    async def initialize_history(self, count=Config.INITIAL_HISTORY_SIZE, max_retries=3):
        logger.log_system("正在初始化历史数据（优先使用CSV批量下载）...")

        keno_csv_url = f"https://www.pc28.ai/api/history/keno.csv?nbr={Config.KENO_HISTORY_DOWNLOAD}"
        keno_rows = await self.download_csv_data(keno_csv_url)
        if keno_rows:
            self.keno_cache.clear()
            for row in keno_rows:
                parsed = self._parse_keno_csv_row(row)
                if parsed:
                    self.keno_cache.append(parsed)
            self.save_cache()
            logger.log_system(f"从CSV加载Keno数据 {len(self.keno_cache)} 条")

        kj_csv_url = f"https://www.pc28.ai/api/history/kj.csv?nbr={Config.KJ_HISTORY_DOWNLOAD}"
        kj_rows = await self.download_csv_data(kj_csv_url)
        if kj_rows:
            self.history_cache.clear()
            for row in kj_rows:
                parsed = self._parse_kj_csv_row(row)
                if parsed:
                    self.history_cache.append(parsed)
            self.save_cache()
            logger.log_system(f"从CSV加载开奖数据 {len(self.history_cache)} 条")
            if len(self.history_cache) >= 30:
                return True

        logger.log_system("CSV下载数据不足，回退到API获取...")
        for attempt in range(max_retries):
            if attempt > 0:
                await asyncio.sleep(2)
            test_data = await self.fetch_kj(nbr=1)
            if not test_data:
                logger.log_error(0, "初始化失败", "无法获取开奖数据，请检查网络或API地址")
                continue
            logger.log_system(f"测试获取到一期数据: {test_data[0]}")
            if len(self.history_cache) >= 50:
                logger.log_system(f"使用缓存数据: 开奖 {len(self.history_cache)}条")
                return True
            kj_data = await self.fetch_kj(nbr=count)
            if not kj_data:
                logger.log_error(0, "初始化失败", "无法获取开奖数据")
                continue
            kj_data.sort(key=lambda x: x.get('parsed_time', datetime.now()), reverse=True)
            self.history_cache.clear()
            for item in kj_data:
                if not any(x.get('qihao') == item['qihao'] for x in self.history_cache):
                    self.history_cache.append(item)
            self.save_cache()
            logger.log_system(f"历史数据初始化完成: 开奖 {len(self.history_cache)}条, Keno {len(self.keno_cache)}条")
            return len(self.history_cache) >= 30
        return False

    async def get_latest_result(self):
        latest_api = await self.fetch_kj(nbr=1)
        if not latest_api:
            return None
        latest = latest_api[0]
        if self.history_cache and self.history_cache[0].get('qihao') == latest['qihao']:
            return None
        if not any(x.get('qihao') == latest['qihao'] for x in self.history_cache):
            self.history_cache.appendleft(latest)
            if len(self.history_cache) > Config.CACHE_SIZE:
                self.history_cache.pop()
            self.save_cache()
        return latest

    async def get_latest_keno(self):
        latest = await self.fetch_keno(nbr=1)
        if not latest:
            return None
        latest_item = latest[0]
        if not any(x.get('qihao') == latest_item['qihao'] for x in self.keno_cache):
            self.keno_cache.appendleft(latest_item)
            if len(self.keno_cache) > 5000:
                self.keno_cache.pop()
            self.save_cache()
        return latest_item

    async def get_history(self, count=50):
        return list(self.history_cache)[:count]

    async def get_keno_history(self, count=50):
        return list(self.keno_cache)[:count]

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def get_statistics(self):
        avg = np.mean(self.call_stats['response_times']) if self.call_stats['response_times'] else 0
        success_rate = (self.call_stats['successful_calls'] / self.call_stats['total_calls']) if self.call_stats['total_calls'] else 0
        return {
            '缓存数据量': len(self.history_cache),
            'Keno缓存': len(self.keno_cache),
            '总API调用': self.call_stats['total_calls'],
            '成功调用': self.call_stats['successful_calls'],
            '成功率': f"{success_rate:.1%}",
            '平均响应时间': f"{avg:.2f}秒",
            '最新期号': self.history_cache[0].get('qihao') if self.history_cache else '无'
        }

# ==================== 数据模型 ====================
@dataclass
class BetParams:
    base_amount: int = Config.DEFAULT_BASE_AMOUNT
    max_amount: int = Config.DEFAULT_MAX_AMOUNT
    multiplier: float = Config.DEFAULT_MULTIPLIER
    stop_loss: int = Config.DEFAULT_STOP_LOSS
    stop_win: int = Config.DEFAULT_STOP_WIN
    stop_balance: int = Config.DEFAULT_STOP_BALANCE
    resume_balance: int = Config.DEFAULT_RESUME_BALANCE

@dataclass
class Account:
    phone: str
    owner_user_id: int
    created_time: str = field(default_factory=lambda: datetime.now().isoformat())
    is_logged_in: bool = False
    auto_betting: bool = False
    prediction_broadcast: bool = False
    display_name: str = ""
    telegram_user_id: int = 0
    game_group_id: int = 0
    game_group_name: str = ""
    prediction_group_id: int = 0
    prediction_group_name: str = ""
    betting_strategy: str = "保守"
    betting_scheme: str = "组合1"
    bet_params: BetParams = field(default_factory=BetParams)
    balance: float = 0
    initial_balance: float = 0
    session_profit: float = 0
    session_loss: float = 0
    total_profit: float = 0
    total_loss: float = 0
    consecutive_losses: int = 0
    consecutive_wins: int = 0
    total_bets: int = 0
    total_wins: int = 0
    last_bet_time: Optional[str] = None
    last_bet_period: Optional[str] = None
    last_bet_types: List[str] = field(default_factory=list)
    last_bet_amount: int = 0
    last_bet_total: int = 0
    last_prediction: Dict = field(default_factory=dict)
    pending_bet: Optional[Dict] = None
    last_balance_check: Optional[str] = None
    last_balance: float = 0
    input_mode: Optional[str] = None
    input_buffer: str = ""
    stop_reason: Optional[str] = None
    martingale_reset: bool = True
    fibonacci_reset: bool = True
    needs_2fa: bool = False
    login_temp_data: dict = field(default_factory=dict)
    chase_enabled: bool = False
    chase_numbers: List[int] = field(default_factory=list)
    chase_periods: int = 0
    chase_current: int = 0
    chase_amount: int = 0
    chase_stop_reason: Optional[str] = None
    streak_records_double: List[Dict] = field(default_factory=list)
    streak_records_kill: List[Dict] = field(default_factory=list)
    current_streak_type_double: Optional[str] = None
    current_streak_start_double: Optional[str] = None
    current_streak_count_double: int = 0
    current_streak_type_kill: Optional[str] = None
    current_streak_start_kill: Optional[str] = None
    current_streak_count_kill: int = 0
    current_streak_messages: List[Dict] = field(default_factory=list)

    recommend_mode: bool = False          # 是否启用动态推荐模式
    risk_profile: str = "稳定"             # 风险偏好：保守、稳定、激进

    last_message_id: Optional[int] = None
    prediction_content: str = "double"
    broadcast_stop_requested: bool = False

    def get_display_name(self) -> str:
        return self.display_name if self.display_name else self.phone

    def get_risk_factor(self) -> float:
        """获取当前风险系数"""
        return Config.RISK_PROFILES.get(self.risk_profile, 0.01)

# ==================== 账户管理器 ====================
class AccountManager:
    def __init__(self):
        self.accounts_file = Config.DATA_DIR / "accounts.json"
        self.user_states_file = Config.DATA_DIR / "user_states.json"
        self.accounts: Dict[str, Account] = {}
        self.user_states: Dict[int, Dict] = {}
        self.clients: Dict[str, TelegramClient] = {}
        self.login_sessions: Dict[str, Dict] = {}
        self.update_lock = asyncio.Lock()
        self.account_locks: Dict[str, asyncio.Lock] = {}
        self.balance_cache: Dict[str, Dict] = {}
        self._dirty: Set[str] = set()
        self._save_task: Optional[asyncio.Task] = None
        self.load_accounts()
        self.load_user_states()
        logger.log_system(f"账户管理器初始化完成，已加载 {len(self.accounts)} 个账户")

    def load_accounts(self):
        if self.accounts_file.exists():
            try:
                with open(self.accounts_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for phone, acc_dict in data.items():
                    acc_dict.pop('is_listening', None)
                    bet_params_dict = acc_dict.get('bet_params', {})
                    bet_params = BetParams(**bet_params_dict)
                    acc_dict['bet_params'] = bet_params

                    if 'needs_2fa' not in acc_dict:
                        acc_dict['needs_2fa'] = False
                    if 'login_temp_data' not in acc_dict:
                        acc_dict['login_temp_data'] = {}

                    if 'chase_enabled' not in acc_dict:
                        acc_dict['chase_enabled'] = False
                        acc_dict['chase_numbers'] = []
                        acc_dict['chase_periods'] = 0
                        acc_dict['chase_current'] = 0
                        acc_dict['chase_amount'] = 0
                        acc_dict['chase_stop_reason'] = None

                    if 'recommend_mode' not in acc_dict:
                        acc_dict['recommend_mode'] = False
                    if 'risk_profile' not in acc_dict:
                        acc_dict['risk_profile'] = "稳定"
                    if 'streak_records_double' not in acc_dict:
                        acc_dict['streak_records_double'] = []
                    if 'streak_records_kill' not in acc_dict:
                        acc_dict['streak_records_kill'] = []
                    if 'current_streak_type_double' not in acc_dict:
                        acc_dict['current_streak_type_double'] = None
                    if 'current_streak_start_double' not in acc_dict:
                        acc_dict['current_streak_start_double'] = None
                    if 'current_streak_count_double' not in acc_dict:
                        acc_dict['current_streak_count_double'] = 0
                    if 'current_streak_type_kill' not in acc_dict:
                        acc_dict['current_streak_type_kill'] = None
                    if 'current_streak_start_kill' not in acc_dict:
                        acc_dict['current_streak_start_kill'] = None
                    if 'current_streak_count_kill' not in acc_dict:
                        acc_dict['current_streak_count_kill'] = 0
                    if 'current_streak_messages' not in acc_dict:
                        acc_dict['current_streak_messages'] = []

                    if 'last_message_id' not in acc_dict:
                        acc_dict['last_message_id'] = None
                    if 'prediction_content' not in acc_dict:
                        acc_dict['prediction_content'] = "double"
                    if 'broadcast_stop_requested' not in acc_dict:
                        acc_dict['broadcast_stop_requested'] = False

                    self.accounts[phone] = Account(**acc_dict)
            except Exception as e:
                logger.log_error(0, "加载账户数据失败", e)

    async def save_accounts(self):
        data = {}
        for phone, acc in self.accounts.items():
            acc_dict = asdict(acc)
            if isinstance(acc_dict.get('current_streak_start_double'), datetime):
                acc_dict['current_streak_start_double'] = acc_dict['current_streak_start_double'].isoformat()
            if isinstance(acc_dict.get('current_streak_start_kill'), datetime):
                acc_dict['current_streak_start_kill'] = acc_dict['current_streak_start_kill'].isoformat()
            for record_list in ['streak_records_double', 'streak_records_kill']:
                for record in acc_dict.get(record_list, []):
                    if isinstance(record.get('start_time'), datetime):
                        record['start_time'] = record['start_time'].isoformat()
                    if isinstance(record.get('end_time'), datetime):
                        record['end_time'] = record['end_time'].isoformat()
            data[phone] = acc_dict
        try:
            async with aiofiles.open(self.accounts_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception as e:
            logger.log_error(0, "保存账户数据失败", e)

    async def _periodic_save(self):
        while True:
            await asyncio.sleep(Config.ACCOUNT_SAVE_INTERVAL)
            dirty = None
            async with self.update_lock:
                if self._dirty:
                    dirty = self._dirty.copy()
                    self._dirty.clear()
            if dirty:
                logger.log_system(f"批量保存 {len(dirty)} 个账户")
                await self.save_accounts()

    def load_user_states(self):
        if self.user_states_file.exists():
            try:
                with open(self.user_states_file, 'r', encoding='utf-8') as f:
                    self.user_states = json.load(f)
            except Exception as e:
                logger.log_error(0, "加载用户状态失败", e)

    def save_user_states(self):
        try:
            with open(self.user_states_file, 'w', encoding='utf-8') as f:
                json.dump(self.user_states, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.log_error(0, "保存用户状态失败", e)

    async def add_account(self, user_id, phone) -> Tuple[bool, str]:
        async with self.update_lock:
            if user_id not in Config.ADMIN_USER_IDS:
                user_accounts = [acc for acc in self.accounts.values() if acc.owner_user_id == user_id]
                if len(user_accounts) >= Config.MAX_ACCOUNTS_PER_USER:
                    return False, f"每个用户最多只能添加 {Config.MAX_ACCOUNTS_PER_USER} 个账户"
            if phone in self.accounts:
                return False, "账户已存在"
            if not re.match(r'^\+\d{10,15}$', phone):
                return False, "手机号格式不正确，需包含国际区号，如 +861234567890"
            self.accounts[phone] = Account(phone=phone, owner_user_id=user_id)
            self._dirty.add(phone)
            logger.log_account(user_id, phone, "添加账户")
            return True, f"账户 {phone} 添加成功"

    def get_account(self, phone) -> Optional[Account]:
        return self.accounts.get(phone)

    async def update_account(self, phone, **kwargs):
        async with self.update_lock:
            if phone not in self.account_locks:
                self.account_locks[phone] = asyncio.Lock()
        async with self.account_locks[phone]:
            if phone in self.accounts:
                acc = self.accounts[phone]
                for k, v in kwargs.items():
                    if k == 'bet_params' and isinstance(v, dict):
                        for pk, pv in v.items():
                            setattr(acc.bet_params, pk, pv)
                    else:
                        setattr(acc, k, v)
                async with self.update_lock:
                    self._dirty.add(phone)
                return True
            return False

    def get_user_accounts(self, user_id):
        return [acc for acc in self.accounts.values() if acc.owner_user_id == user_id]

    def set_user_state(self, user_id, state, data=None):
        self.user_states.setdefault(user_id, {})
        self.user_states[user_id]['state'] = state
        if data:
            self.user_states[user_id].update(data)
        self.user_states[user_id]['last_update'] = datetime.now().isoformat()
        self.save_user_states()

    def get_user_state(self, user_id):
        return self.user_states.get(user_id, {})

    def set_login_session(self, phone, session_data):
        self.login_sessions[phone] = session_data

    def get_login_session(self, phone):
        return self.login_sessions.get(phone)

    def create_client(self, phone):
        try:
            session_name = phone.replace('+', '')
            session_path = Config.SESSIONS_DIR / session_name
            client = TelegramClient(str(session_path), Config.API_ID, Config.API_HASH)
            self.clients[phone] = client
            return client
        except Exception as e:
            logger.log_error(0, f"创建客户端失败 {phone}", e)
            return None

    async def ensure_client_connected(self, phone):
        client = self.clients.get(phone)
        if not client:
            logger.log_error(0, f"客户端不存在 {phone}", None)
            await self.update_account(phone, is_logged_in=False, auto_betting=False, prediction_broadcast=False)
            return False

        if not client.is_connected():
            try:
                await client.connect()
            except Exception as e:
                logger.log_error(0, f"重连客户端失败 {phone}", e)
                await self.update_account(phone, is_logged_in=False, auto_betting=False, prediction_broadcast=False)
                return False

        try:
            if not await client.is_user_authorized():
                logger.log_error(0, f"客户端 {phone} 会话未授权", None)
                await self.update_account(phone, is_logged_in=False, auto_betting=False, prediction_broadcast=False)
                return False
        except Exception as e:
            logger.log_error(0, f"检查授权失败 {phone}", e)
            await self.update_account(phone, is_logged_in=False, auto_betting=False, prediction_broadcast=False)
            return False

        return True

    def get_cached_balance(self, phone):
        cache = self.balance_cache.get(phone)
        if cache and (datetime.now() - cache['time']).seconds < Config.BALANCE_CACHE_SECONDS:
            return cache['balance']
        return None

    def update_balance_cache(self, phone, balance):
        self.balance_cache[phone] = {'balance': balance, 'time': datetime.now()}

    async def verify_login_status(self):
        for phone, acc in self.accounts.items():
            if acc.is_logged_in:
                connected = await self.ensure_client_connected(phone)
                if not connected:
                    logger.log_system(f"账户 {phone} 连接失效，已标记为未登录")

    async def reset_auto_flags_on_start(self):
        for phone, acc in self.accounts.items():
            if acc.auto_betting or acc.prediction_broadcast:
                logger.log_system(f"启动重置账户 {phone} 的自动状态: auto_betting={acc.auto_betting}, broadcast={acc.prediction_broadcast}")
                await self.update_account(phone, auto_betting=False, prediction_broadcast=False)
        logger.log_system("已重置所有账户的自动投注和播报标志，请手动开启所需功能")

    async def start_periodic_save(self):
        self._save_task = asyncio.create_task(self._periodic_save())

    async def stop_periodic_save(self):
        if self._save_task:
            self._save_task.cancel()
            try:
                await self._save_task
            except asyncio.CancelledError:
                pass

# ==================== 金额管理器 ====================
class AmountManager:
    def __init__(self, account_manager):
        self.account_manager = account_manager

    async def set_param(self, phone, param_name, amount, user_id):
        if amount < 0:
            return False, "金额不能为负数"
        acc = self.account_manager.get_account(phone)
        if not acc:
            return False, "账户不存在"
        valid_params = ['base_amount', 'max_amount', 'stop_loss', 'stop_win', 'stop_balance', 'resume_balance']
        if param_name not in valid_params:
            return False, f"无效参数，可选: {', '.join(valid_params)}"

        # 如果设置了基础金额，自动关闭推荐模式
        if param_name == 'base_amount':
            if amount > acc.balance:
                return False, f"基础金额不能超过当前余额 {acc.balance:.2f}KK"
            await self.account_manager.update_account(phone, recommend_mode=False)

        await self.account_manager.update_account(phone, bet_params={param_name: amount})
        logger.log_betting(user_id, "设置金额参数", f"账户:{phone} {param_name}={amount}")
        return True, f"{param_name} 已设置为 {amount}KK"

    async def recommend_base_amount(self, phone: str, balance: float, confidence: int) -> Tuple[bool, str, int]:
        if balance <= 0:
            return False, "余额无效，无法推荐", 0
        risk = Config.RECOMMEND_BASE_RISK + (confidence / 100) * Config.RECOMMEND_RISK_RANGE
        recommended = int(balance * risk)
        recommended = max(Config.MIN_BET_AMOUNT, min(Config.MAX_BET_AMOUNT, recommended))
        return True, f"推荐基础金额: {recommended} KK (基于余额 {balance:.0f}KK 和置信度 {confidence}%)", recommended

# ==================== 策略管理器 ====================
class BettingStrategyManager:
    def __init__(self, account_manager):
        self.account_manager = account_manager
        self.strategies = {
            '保守': {'description': '保守策略', 'base_amount': 10000, 'max_amount': 100000,
                    'multiplier': 1.5, 'stop_loss': 100000, 'stop_win': 50000,
                    'stop_balance': 50000, 'resume_balance': 200000},
            '平衡': {'description': '平衡策略', 'base_amount': 50000, 'max_amount': 500000,
                    'multiplier': 2.0, 'stop_loss': 500000, 'stop_win': 250000,
                    'stop_balance': 100000, 'resume_balance': 500000},
            '激进': {'description': '激进策略', 'base_amount': 100000, 'max_amount': 1000000,
                    'multiplier': 2.5, 'stop_loss': 1000000, 'stop_win': 500000,
                    'stop_balance': 200000, 'resume_balance': 1000000},
            '马丁格尔': {'description': '马丁格尔策略', 'base_amount': 10000, 'max_amount': 10000000,
                        'multiplier': 2.0, 'stop_loss': 5000000, 'stop_win': 1000000,
                        'stop_balance': 500000, 'resume_balance': 2000000},
            '斐波那契': {'description': '斐波那契策略', 'base_amount': 10000, 'max_amount': 10000000,
                        'multiplier': 1.0, 'stop_loss': 5000000, 'stop_win': 1000000,
                        'stop_balance': 500000, 'resume_balance': 2000000},
        }
        self.schemes = {
            '组合1': '投注第1推荐组合',
            '组合2': '投注第2推荐组合',
            '组合1+2': '同时投注第1、2推荐组合',
            '杀主': '投注除最不可能组合外的所有组合'
        }

    async def set_strategy(self, phone, strategy_name, user_id):
        if strategy_name not in self.strategies:
            return False, f"无效策略"
        cfg = self.strategies[strategy_name]
        # 同时更新风险偏好（如果策略名称匹配）
        if strategy_name in ['保守', '平衡', '激进']:
            await self.account_manager.update_account(phone, risk_profile=strategy_name)
        await self.account_manager.update_account(
            phone,
            betting_strategy=strategy_name,
            bet_params={
                'base_amount': cfg['base_amount'],
                'max_amount': cfg['max_amount'],
                'multiplier': cfg['multiplier'],
                'stop_loss': cfg['stop_loss'],
                'stop_win': cfg['stop_win'],
                'stop_balance': cfg.get('stop_balance', 0),
                'resume_balance': cfg.get('resume_balance', 100000),
            }
        )
        logger.log_betting(user_id, "设置策略", f"账户:{phone} 策略:{strategy_name}")
        return True, f"已设置为: {strategy_name} 策略\n{cfg['description']}"

    async def set_scheme(self, phone, scheme_name, user_id):
        if scheme_name not in self.schemes:
            return False, f"无效方案"
        await self.account_manager.update_account(phone, betting_scheme=scheme_name)
        logger.log_betting(user_id, "设置方案", f"账户:{phone} 方案:{scheme_name}")
        return True, f"投注方案已设置为: {scheme_name} ({self.schemes[scheme_name]})"

# ==================== 预测播报器 ====================
class PredictionBroadcaster:
    def __init__(self, account_manager, model_manager, api_client, global_scheduler):
        self.account_manager = account_manager
        self.model = model_manager
        self.api = api_client
        self.global_scheduler = global_scheduler
        self.broadcast_tasks = {}
        self.global_predictions = {
            'predictions': [],
            'last_open_qihao': None,
            'next_qihao': None,
            'last_update': None,
            'cached_double_message': None,
            'cached_kill_message': None
        }
        self.last_sent_qihao = {}
        self._send_locks = {}
        self.stop_target_qihao = {}

    async def start_broadcast(self, phone, user_id):
        acc = self.account_manager.get_account(phone)
        if not acc:
            return False, "账户不存在"
        if not acc.is_logged_in:
            return False, "请先登录账户"
        if not acc.prediction_group_id:
            return False, "请先设置播报群"
        if acc.broadcast_stop_requested:
            await self.account_manager.update_account(phone, broadcast_stop_requested=False)
            self.stop_target_qihao.pop(phone, None)
        if phone in self.broadcast_tasks and not self.broadcast_tasks[phone].done():
            return True, "播报器已在运行"
        if phone in self.broadcast_tasks:
            self.broadcast_tasks[phone].cancel()
        self.last_sent_qihao[phone] = self.global_predictions.get('next_qihao')
        task = self.global_scheduler._create_task(self._broadcast_loop(phone, acc.prediction_group_id))
        self.broadcast_tasks[phone] = task
        await self.account_manager.update_account(phone, prediction_broadcast=True)
        logger.log_prediction(user_id, "播报器启动", f"账户:{phone}")
        return True, "预测播报器启动成功"

    async def stop_broadcast(self, phone, user_id):
        acc = self.account_manager.get_account(phone)
        if not acc:
            return False, "账户不存在"
        if not acc.prediction_broadcast:
            return True, "播报器已停止"
        target = self.global_predictions.get('next_qihao')
        await self.account_manager.update_account(phone, broadcast_stop_requested=True)
        self.stop_target_qihao[phone] = target
        logger.log_prediction(user_id, "播报器平滑停止请求", f"账户:{phone} 目标期号:{target}")
        return True, "将在最后一期开奖后停止播报"

    async def _broadcast_loop(self, phone, group_id):
        error_count = 0
        target_qihao = None
        while True:
            try:
                acc = self.account_manager.get_account(phone)
                if not acc:
                    break

                if acc.broadcast_stop_requested:
                    if target_qihao is None:
                        target_qihao = self.stop_target_qihao.get(phone)
                    if target_qihao is None:
                        target_qihao = self.global_predictions.get('next_qihao')
                    last_sent = self.last_sent_qihao.get(phone)
                    if last_sent != target_qihao:
                        msg_id = await self.send_prediction(phone, group_id, force_qihao=target_qihao)
                        if msg_id is None:
                            await self.account_manager.update_account(phone, prediction_broadcast=False, broadcast_stop_requested=False)
                            self.last_sent_qihao.pop(phone, None)
                            self._send_locks.pop(phone, None)
                            self.stop_target_qihao.pop(phone, None)
                            break
                        last_sent = target_qihao
                    last_open = self.global_predictions.get('last_open_qihao')
                    if last_open == target_qihao:
                        await self.account_manager.update_account(phone, prediction_broadcast=False, broadcast_stop_requested=False)
                        self.last_sent_qihao.pop(phone, None)
                        self._send_locks.pop(phone, None)
                        self.stop_target_qihao.pop(phone, None)
                        break
                elif not acc.prediction_broadcast:
                    self.last_sent_qihao.pop(phone, None)
                    self._send_locks.pop(phone, None)
                    self.stop_target_qihao.pop(phone, None)
                    break
                else:
                    await self.send_prediction(phone, group_id)

                error_count = 0
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                self.last_sent_qihao.pop(phone, None)
                self._send_locks.pop(phone, None)
                self.stop_target_qihao.pop(phone, None)
                break
            except Exception as e:
                error_count += 1
                logger.log_error(0, f"播报器循环异常 {phone}", e)
                if error_count >= 5:
                    await self.account_manager.update_account(phone, prediction_broadcast=False, broadcast_stop_requested=False)
                    self.last_sent_qihao.pop(phone, None)
                    self._send_locks.pop(phone, None)
                    self.stop_target_qihao.pop(phone, None)
                    break
                await asyncio.sleep(10)

    async def update_global_predictions(self, prediction, next_qihao, latest):
        last_correct_double = None
        last_correct_kill = None
        current_open_qihao = latest.get('qihao')
        current_sum = latest.get('sum')
        current_combo = latest.get('combo')

        matched_pred = None
        for p in self.global_predictions['predictions']:
            if p.get('qihao') == current_open_qihao:
                matched_pred = p
                break

        if matched_pred:
            matched_pred['actual'] = current_combo
            matched_pred['sum'] = current_sum
            matched_pred['correct_double'] = (matched_pred['main'] == current_combo or matched_pred['candidate'] == current_combo)
            matched_pred['correct_kill'] = (matched_pred['kill'] != current_combo)
            last_correct_double = matched_pred['correct_double']
            last_correct_kill = matched_pred['correct_kill']
            await self.model.learn(matched_pred, current_combo, current_open_qihao, current_sum)
            self.model.pattern_recognizer.learn_pattern(await self.api.get_history(50))
            self.model.long_term_memory.learn(await self.api.get_history(1))

        kill_combo = prediction['kill']
        existing = None
        for i, p in enumerate(self.global_predictions['predictions']):
            if p.get('qihao') == next_qihao:
                existing = p
                break

        new_pred = {
            'qihao': next_qihao,
            'main': prediction['main'],
            'candidate': prediction['candidate'],
            'kill': prediction['kill'],
            'confidence': prediction['confidence'],
            'time': datetime.now().isoformat(),
            'actual': None,
            'sum': None,
            'correct_double': None,
            'correct_kill': None,
            'message_id': None,
            'algo_details': prediction.get('algo_details', []),
        }

        if existing:
            existing.update(new_pred)
            logger.log_system(f"更新已存在的期号 {next_qihao} 的预测")
        else:
            self.global_predictions['predictions'].append(new_pred)
            if len(self.global_predictions['predictions']) > Config.PREDICTION_HISTORY_SIZE:
                self.global_predictions['predictions'] = self.global_predictions['predictions'][-Config.PREDICTION_HISTORY_SIZE:]

        self.global_predictions['last_open_qihao'] = current_open_qihao
        self.global_predictions['next_qihao'] = next_qihao
        self.global_predictions['last_update'] = datetime.now().isoformat()
        self._update_cached_messages()

        sem = asyncio.Semaphore(Config.MAX_CONCURRENT_PREDICTIONS)

        async def send_and_check(phone, group_id):
            acc = self.account_manager.get_account(phone)
            if not acc:
                return
            msg_id = await self.send_prediction(phone, group_id)
            if msg_id and last_correct_double is not None and last_correct_kill is not None:
                if acc.prediction_content == "double":
                    await self._check_streak(phone, group_id, last_correct_double, msg_id, content_type="double")
                else:
                    await self._check_streak(phone, group_id, last_correct_kill, msg_id, content_type="kill")

        tasks = []
        for phone, task in list(self.broadcast_tasks.items()):
            if not task.done():
                acc = self.account_manager.get_account(phone)
                if acc and (acc.prediction_broadcast or acc.broadcast_stop_requested) and acc.prediction_group_id:
                    tasks.append(send_and_check(phone, acc.prediction_group_id))
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    logger.log_error(0, "并发发送预测异常", res)

    def _update_cached_messages(self):
        lines = ["🤖强化学习中 "]
        lines.append("-" * 30)
        lines.append("期号    主推候选  状态  和值")
        for p in self.global_predictions['predictions'][-Config.PREDICTION_HISTORY_SIZE:]:
            q = p['qihao'][-4:] if len(p['qihao']) >= 4 else p['qihao']
            combo_str = p['main'] + p['candidate']
            mark = "✅" if p.get('correct_double') is True else "❌" if p.get('correct_double') is False else "⏳"
            s = str(p['sum']) if p['sum'] is not None else "--"
            lines.append(f"{q:4s}   {combo_str:4s}   {mark:2s}   {s:>2s}")
        self.global_predictions['cached_double_message'] = "AI双组预测\n```" + "\n".join(lines) + "\n```"

        kill_lines = ["🤖keno暗线匹配灰盒杀"]
        kill_lines.append("-" * 30)
        kill_lines.append("期号    杀组    状态  和值")
        for p in self.global_predictions['predictions'][-Config.PREDICTION_HISTORY_SIZE:]:
            q = p['qihao'][-4:] if len(p['qihao']) >= 4 else p['qihao']
            kill = p.get('kill', '--')
            if kill is None:
                kill = '--'
            mark = "✅" if p.get('correct_kill') is True else "❌" if p.get('correct_kill') is False else "⏳"
            s = str(p['sum']) if p['sum'] is not None else "--"
            kill_lines.append(f"{q:4s}   {kill:4s}   {mark:2s}   {s:>2s}")
        self.global_predictions['cached_kill_message'] = "AI杀组预测\n```" + "\n".join(kill_lines) + "\n```"

    async def _check_streak(self, phone, group_id, is_correct, last_message_id, content_type: str):
        acc = self.account_manager.get_account(phone)
        if not acc:
            return

        if content_type == "double":
            current_type = acc.current_streak_type_double
            current_start = acc.current_streak_start_double
            current_count = acc.current_streak_count_double
            streak_records = acc.streak_records_double
        else:
            current_type = acc.current_streak_type_kill
            current_start = acc.current_streak_start_kill
            current_count = acc.current_streak_count_kill
            streak_records = acc.streak_records_kill

        now = datetime.now().isoformat()

        if is_correct:
            if current_type == "win":
                new_count = current_count + 1
            else:
                current_type = "win"
                current_start = now
                new_count = 1
        else:
            if current_type == "loss":
                new_count = current_count + 1
            else:
                current_type = "loss"
                current_start = now
                new_count = 1

        logger.log_prediction(0, f"连输连赢更新",
            f"账户:{phone} 类型:{content_type} {current_type} 计数:{new_count}")

        if new_count in [7, 8, 10]:
            message_link = f"https://t.me/c/{str(group_id).replace('-100', '')}/{last_message_id}"
            record = {
                'type': current_type,
                'count': new_count,
                'start_time': current_start,
                'end_time': now,
                'message_link': message_link,
                'message_id': last_message_id,
                'group_id': group_id
            }
            streak_records.append(record)
            if len(streak_records) > 50:
                streak_records = streak_records[-50:]

            if new_count == 10:
                current_type = None
                current_start = None
                new_count = 0

        if content_type == "double":
            await self.account_manager.update_account(phone,
                current_streak_type_double=current_type,
                current_streak_start_double=current_start,
                current_streak_count_double=new_count,
                streak_records_double=streak_records
            )
        else:
            await self.account_manager.update_account(phone,
                current_streak_type_kill=current_type,
                current_streak_start_kill=current_start,
                current_streak_count_kill=new_count,
                streak_records_kill=streak_records
            )

    async def send_prediction(self, phone, group_id, force_qihao=None):
        lock = self._send_locks.setdefault(phone, asyncio.Lock())
        async with lock:
            current_next_qihao = self.global_predictions.get('next_qihao')
            target_qihao = force_qihao if force_qihao is not None else current_next_qihao
            if self.last_sent_qihao.get(phone) == target_qihao:
                return None

            client = self.account_manager.clients.get(phone)
            if not client:
                return None
            if not await self.account_manager.ensure_client_connected(phone):
                logger.log_error(0, f"客户端未连接，无法发送播报 {phone}", None)
                return None

            acc = self.account_manager.get_account(phone)
            if not acc:
                return None

            if acc.prediction_content == "double":
                message = self.global_predictions.get('cached_double_message')
                if not message:
                    self._update_cached_messages()
                    message = self.global_predictions['cached_double_message']
            else:
                message = self.global_predictions.get('cached_kill_message')
                if not message:
                    self._update_cached_messages()
                    message = self.global_predictions['cached_kill_message']

            max_retries = 3
            for retry in range(max_retries):
                try:
                    msg = await client.send_message(group_id, message, parse_mode='markdown')
                    await self.account_manager.update_account(phone, last_message_id=msg.id)
                    self.last_sent_qihao[phone] = target_qihao
                    if self.global_predictions['predictions']:
                        self.global_predictions['predictions'][-1]['message_id'] = msg.id
                    return msg.id
                except FloodWaitError as e:
                    wait_seconds = e.seconds
                    logger.log_prediction(0, f"播报触发限流，等待 {wait_seconds} 秒", f"账户:{phone}")
                    if retry < max_retries - 1:
                        await asyncio.sleep(min(wait_seconds, 30))
                    else:
                        logger.log_error(0, f"播报发送失败（限流）", e)
                except Exception as e:
                    logger.log_error(0, f"发送播报失败", e)
                    break
            return None

# ==================== 游戏调度器 ====================
class GameScheduler:
    def __init__(self, account_manager, model_manager, api_client):
        self.account_manager = account_manager
        self.model = model_manager
        self.api = api_client
        self.game_stats = {
            'total_cycles': 0, 'betting_cycles': 0,
            'successful_bets': 0, 'failed_bets': 0,
            'total_profit': 0, 'total_loss': 0
        }

    async def start_auto_betting(self, phone, user_id):
        acc = self.account_manager.get_account(phone)
        if not acc:
            return False, "账户不存在"
        if not acc.is_logged_in:
            return False, "请先登录账户"
        if not acc.game_group_id:
            return False, "请先设置游戏群"
        await self.account_manager.update_account(phone,
            auto_betting=True,
            martingale_reset=True,
            fibonacci_reset=True
        )
        logger.log_betting(user_id, "自动投注开启", f"账户:{phone}")
        return True, "自动投注已开启"

    async def stop_auto_betting(self, phone, user_id):
        await self.account_manager.update_account(phone, auto_betting=False)
        logger.log_betting(user_id, "自动投注关闭", f"账户:{phone}")
        return True, "自动投注已关闭"

    async def check_bet_result(self, phone, expected_qihao, latest_result):
        acc = self.account_manager.get_account(phone)
        if not acc:
            return

        scheme = acc.betting_scheme
        last_pred = acc.last_prediction
        last_bet_types = acc.last_bet_types

        if not last_pred or not last_bet_types:
            return

        actual_combo = latest_result.get('combo')
        if not actual_combo:
            return

        main = last_pred.get('main')
        candidate = last_pred.get('candidate')

        def is_match(bet_type: str, actual: str) -> bool:
            if bet_type == actual:
                return True
            if bet_type in ["大", "小"] and actual.startswith(bet_type):
                return True
            if bet_type in ["单", "双"]:
                if actual == bet_type:
                    return True
                if len(actual) >= 2 and actual[1] == bet_type:
                    return True
            return False

        is_win = False
        if scheme == '组合1':
            is_win = is_match(main, actual_combo)
        elif scheme == '组合2':
            is_win = is_match(candidate, actual_combo)
        elif scheme == '组合1+2':
            is_win = is_match(main, actual_combo) or is_match(candidate, actual_combo)
        else:
            is_win = any(is_match(t, actual_combo) for t in last_bet_types)

        if is_win:
            await self.account_manager.update_account(phone,
                consecutive_wins=acc.consecutive_wins + 1,
                consecutive_losses=0,
                martingale_reset=True,
                fibonacci_reset=True,
                total_wins=acc.total_wins + 1
            )
            logger.log_betting(0, "投注命中",
                f"账户:{phone} 期号:{expected_qihao} 实际:{actual_combo} 方案:{scheme} 主推:{main} 候选:{candidate}")
        else:
            await self.account_manager.update_account(phone,
                consecutive_losses=acc.consecutive_losses + 1,
                consecutive_wins=0,
            )
            logger.log_betting(0, "投注未命中",
                f"账户:{phone} 期号:{expected_qihao} 实际:{actual_combo} 方案:{scheme} 主推:{main} 候选:{candidate}")

    async def execute_chase(self, phone: str, latest: dict):
        acc = self.account_manager.get_account(phone)
        if not acc or not acc.chase_enabled:
            return

        logger.log_betting(0, "追号检查",
            f"账户:{phone} 启用:{acc.chase_enabled} 进度:{acc.chase_current}/{acc.chase_periods}")

        if acc.chase_current >= acc.chase_periods:
            await self.account_manager.update_account(phone,
                chase_enabled=False,
                chase_stop_reason="期满",
                chase_numbers=[],
                chase_periods=0,
                chase_current=0,
                chase_amount=0
            )
            logger.log_betting(0, "追号期满停止", f"账户:{phone}")
            return

        current_qihao = latest.get('qihao')
        if acc.last_bet_period == current_qihao:
            logger.log_betting(0, "追号跳过（已投注本期）", f"账户:{phone}")
            return

        bet_amount = acc.chase_amount if acc.chase_amount > 0 else acc.bet_params.base_amount
        bet_amount = min(bet_amount, acc.bet_params.max_amount)
        bet_amount = max(bet_amount, Config.MIN_BET_AMOUNT)

        bet_items = [f"{num}/{bet_amount}" for num in acc.chase_numbers]
        if not bet_items:
            logger.log_betting(0, "追号无有效数字", f"账户:{phone}")
            return

        total_needed = bet_amount * len(acc.chase_numbers)

        cur_bal = await self._query_balance(phone)
        if cur_bal is None or cur_bal < total_needed:
            logger.log_betting(0, "追号余额不足",
                f"账户:{phone} 需要:{total_needed} 余额:{cur_bal if cur_bal else '未知'}")
            return

        success = await self._send_bets(phone, bet_items, is_chase=True)
        if success:
            new_current = acc.chase_current + 1
            await self.account_manager.update_account(phone,
                chase_current=new_current,
                last_bet_period=current_qihao,
                last_bet_types=[str(num) for num in acc.chase_numbers],
                last_bet_amount=bet_amount,
                last_bet_total=total_needed
            )
            logger.log_betting(0, "追号成功",
                f"账户:{phone} 数字:{acc.chase_numbers} 金额:{bet_amount} 进度:{new_current}/{acc.chase_periods}")

            asyncio.create_task(self._query_balance(phone))

    async def execute_bet(self, phone, prediction, latest):
        acc = self.account_manager.get_account(phone)
        if not acc:
            return

        await self.execute_chase(phone, latest)

        if not acc.auto_betting:
            return

        if not await self.account_manager.ensure_client_connected(phone):
            logger.log_betting(0, f"账户 {phone} 客户端未连接，无法投注", "")
            return

        current_qihao = latest.get('qihao')
        if acc.last_bet_period == current_qihao:
            return

        now = datetime.now()
        next_open = latest['parsed_time'] + timedelta(seconds=Config.GAME_CYCLE_SECONDS)
        close_time = next_open - timedelta(seconds=Config.CLOSE_BEFORE_SECONDS)
        if now >= close_time:
            logger.log_betting(0, "已封盘，跳过投注", f"账户:{phone}")
            return

        cur_bal = await self.get_balance(phone)
        if cur_bal is None:
            logger.log_betting(0, "无法获取余额，跳过投注", f"账户:{phone}")
            return

        if cur_bal <= 0:
            logger.log_betting(0, "余额为零或负数，跳过投注", f"账户:{phone}")
            return

        # 如果启用了推荐模式，动态计算基础金额
        if acc.recommend_mode:
            risk = acc.get_risk_factor()
            dynamic_base = int(cur_bal * risk)
            dynamic_base = max(Config.MIN_BET_AMOUNT, min(acc.bet_params.max_amount, dynamic_base))
            # 临时覆盖 bet_params.base_amount 仅用于本次计算
            temp_base = dynamic_base
        else:
            temp_base = acc.bet_params.base_amount

        # 计算投注金额（使用临时基础金额）
        bet_amount, updates = self._calculate_bet_amount_with_base(acc, temp_base)
        if updates:
            await self.account_manager.update_account(phone, **updates)

        if acc.betting_scheme == '杀主':
            kill_combo = prediction['kill']
            bet_types = [c for c in COMBOS if c != kill_combo]
            logger.log_betting(0, f"杀主投注: 避开 {kill_combo}, 投注组合: {bet_types}", f"账户:{phone}")
        else:
            bet_types = self._get_bet_types(prediction, acc.betting_scheme)

        bet_items = [f"{t} {bet_amount}" for t in bet_types]
        total = bet_amount * len(bet_types)
        if cur_bal < total:
            logger.log_betting(0, "余额不足", f"账户:{phone} 余额:{cur_bal} 需要:{total}")
            return

        success = await self._send_bets(phone, bet_items, is_chase=False)
        if success:
            self.game_stats['successful_bets'] += 1
            self.game_stats['betting_cycles'] += 1
            await self.account_manager.update_account(phone,
                last_bet_time=datetime.now().isoformat(),
                last_bet_amount=bet_amount,
                last_bet_types=bet_types,
                total_bets=acc.total_bets + 1,
                last_bet_total=total,
                last_prediction={
                    'main': prediction['main'],
                    'candidate': prediction['candidate'],
                    'confidence': prediction['confidence'],
                    'kill': prediction['kill']
                },
                last_bet_period=current_qihao
            )
            logger.log_betting(0, "投注成功",
                f"账户:{phone} 每注金额:{bet_amount} 总金额:{total} 类型:{bet_types} 置信度:{prediction['confidence']:.1f}%")
            asyncio.create_task(self._query_balance(phone))
        else:
            self.game_stats['failed_bets'] += 1
            logger.log_betting(0, "投注失败", f"账户:{phone}")

    def _calculate_bet_amount_with_base(self, acc: Account, base_override: int) -> Tuple[int, Dict]:
        """根据策略和连输连赢计算投注金额，允许临时覆盖基础金额"""
        base = base_override
        max_amt = acc.bet_params.max_amount
        losses = acc.consecutive_losses
        wins = acc.consecutive_wins
        mult = acc.bet_params.multiplier
        strategy = acc.betting_strategy
        updates = {}

        if strategy == '马丁格尔':
            if wins > 0 or acc.martingale_reset:
                amt = base
                updates['martingale_reset'] = False
            else:
                amt = base * (mult ** losses)
        elif strategy == '斐波那契':
            if wins > 0 or acc.fibonacci_reset:
                amt = base
                updates['fibonacci_reset'] = False
            else:
                fib = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
                idx = min(losses, len(fib)-1)
                amt = base * fib[idx]
        elif strategy == '激进':
            amt = base * (1 + losses)
        else:
            amt = base

        amt = min(amt, max_amt)
        amt = max(amt, Config.MIN_BET_AMOUNT)
        return int(amt), updates

    def _calculate_bet_amount(self, acc: Account) -> Tuple[int, Dict]:
        return self._calculate_bet_amount_with_base(acc, acc.bet_params.base_amount)

    def _get_bet_types(self, pred: Dict, scheme: str) -> List[str]:
        rec = [pred['main'], pred['candidate']]
        if pred['main'] == pred['candidate']:
            rec = [pred['main']]
        if scheme == '组合1':
            return [rec[0]] if rec else ['小双']
        if scheme == '组合2':
            return [rec[1]] if len(rec) > 1 else ['小双']
        if scheme == '组合1+2':
            return rec[:2] if len(rec) >= 2 else rec
        return [rec[0]] if rec else ['小双']

    async def _send_bets(self, phone: str, bet_items: List[str], is_chase: bool) -> bool:
        client = self.account_manager.clients.get(phone)
        acc = self.account_manager.get_account(phone)
        if not client or not acc:
            logger.log_error(phone, "发送投注失败", "客户端或账户不存在")
            return False

        gid = acc.game_group_id
        if not gid:
            logger.log_error(phone, "发送投注失败", "未设置游戏群")
            return False

        if not await self.account_manager.ensure_client_connected(phone):
            logger.log_error(phone, "发送投注失败", "客户端未连接")
            return False

        message = " ".join(bet_items)
        bet_type = "追号" if is_chase else "自动投注"
        logger.log_betting(0, f"发送{bet_type}", f"账户:{phone} 消息:{message}")

        max_retries = 3
        for retry in range(max_retries):
            try:
                await client.send_message(gid, message)
                logger.log_game(f"{bet_type}发送成功: {phone} -> {message}")
                return True
            except FloodWaitError as e:
                wait_seconds = e.seconds
                logger.log_betting(0, f"触发限流，等待 {wait_seconds} 秒", f"账户:{phone}")
                if retry < max_retries - 1:
                    await asyncio.sleep(min(wait_seconds, 30))
                else:
                    logger.log_error(phone, f"{bet_type}发送失败（限流）", e)
                    try:
                        await client.send_message(gid, "取消")
                        logger.log_game(f"取消指令发送成功: {phone}")
                    except Exception as cancel_e:
                        logger.log_error(phone, f"发送取消指令失败", cancel_e)
                    return False
            except Exception as e:
                logger.log_error(phone, f"{bet_type}发送失败", e)
                try:
                    await client.send_message(gid, "取消")
                    logger.log_game(f"取消指令发送成功: {phone}")
                except Exception as cancel_e:
                    logger.log_error(phone, f"发送取消指令失败", cancel_e)
                return False
        return False

    async def _query_balance(self, phone: str) -> Optional[float]:
        client = self.account_manager.clients.get(phone)
        acc = self.account_manager.get_account(phone)
        if not client or not acc:
            return None

        if not await self.account_manager.ensure_client_connected(phone):
            return None

        try:
            max_retries = 3
            for retry in range(max_retries):
                try:
                    await client.send_message(Config.BALANCE_BOT, "/start")
                    break
                except FloodWaitError as e:
                    wait_seconds = e.seconds
                    logger.log_betting(0, f"余额查询触发限流，等待 {wait_seconds} 秒", f"账户:{phone}")
                    if retry < max_retries - 1:
                        await asyncio.sleep(min(wait_seconds, 30))
                    else:
                        return None

            start_dt = datetime.now()
            balance = None
            while (datetime.now() - start_dt).total_seconds() < 10:
                await asyncio.sleep(1)
                msgs = await client.get_messages(Config.BALANCE_BOT, limit=5)
                for msg in msgs:
                    if msg.text and ('KKCOIN' in msg.text.upper() or '余额' in msg.text):
                        text = msg.text
                        patterns = [
                            r'💰\s*KKCOIN\s*[:：]\s*([\d,]+\.?\d*)',
                            r'KKCOIN\s*[:：]\s*([\d,]+\.?\d*)',
                            r'([\d,]+\.?\d*)\s*KKCOIN',
                            r'余额[:：]\s*([\d,]+\.?\d*)',
                            r'([\d,]+\.?\d*)\s*元'
                        ]
                        for pat in patterns:
                            m = re.search(pat, text, re.IGNORECASE)
                            if m:
                                try:
                                    balance = float(m.group(1).replace(',', ''))
                                    break
                                except:
                                    continue
                        if balance is not None:
                            break
                if balance is not None:
                    break

            if balance is None:
                return None

            self.account_manager.update_balance_cache(phone, balance)

            if acc.initial_balance == 0:
                await self.account_manager.update_account(
                    phone,
                    initial_balance=balance,
                    balance=balance,
                    last_balance=balance,
                    last_balance_check=datetime.now().isoformat()
                )
                return balance

            old = acc.balance
            change = balance - old

            new_profit = acc.total_profit
            new_loss = acc.total_loss
            if change > 0:
                new_profit += change
            elif change < 0:
                new_loss += -change

            await self.account_manager.update_account(phone,
                balance=balance,
                last_balance=old,
                last_balance_check=datetime.now().isoformat(),
                total_profit=new_profit,
                total_loss=new_loss
            )

            if acc.auto_betting:
                if acc.bet_params.stop_balance > 0 and balance < acc.bet_params.stop_balance:
                    await self.stop_auto_betting(phone, 0)
                    await self.account_manager.update_account(phone, stop_reason="余额低于阈值")
                elif acc.bet_params.resume_balance > 0 and balance >= acc.bet_params.resume_balance:
                    if not acc.auto_betting and acc.stop_reason == "余额低于阈值":
                        await self.start_auto_betting(phone, 0)
                        await self.account_manager.update_account(phone, stop_reason=None)
            return balance
        except Exception as e:
            logger.log_error(0, f"查询余额失败 {phone}", e)
            return None

    async def get_balance(self, phone: str) -> Optional[float]:
        cached = self.account_manager.get_cached_balance(phone)
        if cached is not None:
            return cached
        return await self._query_balance(phone)

    async def manual_bet(self, phone: str, bet_type: str, amount: int, user_id: int) -> Tuple[bool, str]:
        acc = self.account_manager.get_account(phone)
        if not acc:
            return False, "账户不存在"
        if not acc.is_logged_in:
            return False, "账户未登录"
        if not acc.game_group_id:
            return False, "未设置游戏群"
        valid = ['大','小','单','双','大单','大双','小单','小双']
        bet_type = bet_type.strip()
        if bet_type not in valid:
            return False, f'无效类型，可选: {valid}'
        if amount < Config.MIN_BET_AMOUNT or amount > Config.MAX_BET_AMOUNT:
            return False, f'金额必须在{Config.MIN_BET_AMOUNT}-{Config.MAX_BET_AMOUNT}KK之间'

        cur_bal = await self.get_balance(phone)
        if cur_bal is None:
            return False, "余额查询失败"
        if cur_bal < amount:
            return False, f"余额不足，当前余额: {cur_bal:.2f}KK"

        latest = await self.api.get_latest_result()
        current_qihao = latest.get('qihao') if latest else None

        success = await self._send_bets(phone, [f"{bet_type} {amount}"], is_chase=False)
        if success:
            await self.account_manager.update_account(phone,
                last_bet_time=datetime.now().isoformat(),
                last_bet_amount=amount,
                last_bet_types=[bet_type],
                total_bets=acc.total_bets + 1,
                last_bet_period=current_qihao
            )
            asyncio.create_task(self._query_balance(phone))
            return True, f'已发送投注: {bet_type} {amount}KK'
        return False, '发送投注失败'

    def get_stats(self):
        auto = sum(1 for a in self.account_manager.accounts.values() if a.auto_betting)
        broadcast = sum(1 for a in self.account_manager.accounts.values() if a.prediction_broadcast)
        return {
            'auto_betting_accounts': auto,
            'broadcast_accounts': broadcast,
            'game_stats': self.game_stats.copy()
        }

# ==================== 全局调度器 ====================
class GlobalScheduler:
    def __init__(self, account_manager, model_manager, api_client,
                 prediction_broadcaster, game_scheduler):
        self.account_manager = account_manager
        self.model = model_manager
        self.api = api_client
        self.prediction_broadcaster = prediction_broadcaster
        self.game_scheduler = game_scheduler
        self.task = None
        self.running = False
        self.last_qihao = None
        self.check_interval = Config.SCHEDULER_CHECK_INTERVAL
        self.health_check_interval = Config.HEALTH_CHECK_INTERVAL
        self.last_health_check = 0
        self.bet_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_BETS)
        self.tasks = set()

        # 覆盖模型的_check_new_result方法
        self.model._check_new_result = self._check_new_result

    def _is_maintenance_time(self, now: datetime) -> bool:
        beijing_time = now + timedelta(hours=8)
        hour = beijing_time.hour
        minute = beijing_time.minute

        is_dst = 4 <= now.month <= 10

        if is_dst:
            return (hour == 19 and minute >= 55) or (hour == 20 and minute <= 30)
        else:
            return (hour == 20 and minute >= 55) or (hour == 21 and minute <= 30)

    async def _download_history_during_maintenance(self):
        try:
            logger.log_system("维护时段：开始下载历史数据...")
            kj_url = f"https://www.pc28.ai/api/history/kj.csv?nbr={Config.KJ_HISTORY_DOWNLOAD}"
            kj_rows = await self.api.download_csv_data(kj_url)
            kj_data = []
            for row in kj_rows:
                parsed = self.api._parse_kj_csv_row(row)
                if parsed:
                    kj_data.append(parsed)
            keno_url = f"https://www.pc28.ai/api/history/keno.csv?nbr={Config.KENO_HISTORY_DOWNLOAD}"
            keno_rows = await self.api.download_csv_data(keno_url)
            keno_data = []
            for row in keno_rows:
                parsed = self.api._parse_keno_csv_row(row)
                if parsed:
                    keno_data.append(parsed)

            if kj_data and keno_data:
                logger.log_system("启动持续训练模式...")
                await self.model.offline_train(kj_data, keno_data, max_epochs=Config.TRAIN_EPOCHS)
        except Exception as e:
            logger.log_error(0, "维护时段下载任务失败", e)

    async def start(self):
        if self.running:
            return
        self.running = True
        self.task = asyncio.create_task(self._run())
        self.tasks.add(self.task)
        logger.log_system("全局调度器已启动")

    async def stop(self):
        self.running = False
        self.tasks = {t for t in self.tasks if not t.done()}
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        logger.log_system("全局调度器已停止")

    def _create_task(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def _run(self):
        init_success = False
        for attempt in range(5):
            if await self.api.initialize_history():
                init_success = True
                break
            logger.log_system(f"历史数据初始化失败，5秒后重试 ({attempt+1}/5)")
            await asyncio.sleep(5)
        if not init_success:
            logger.log_error(0, "全局调度器", "无法初始化历史数据，调度器将继续运行但可能无法预测")

        await self._feed_keno_history()

        while self.running:
            try:
                now = datetime.now()

                if self._is_maintenance_time(now):
                    logger.log_system("当前处于维护时段，暂停实时检测，开始下载历史数据...")
                    asyncio.create_task(self._download_history_during_maintenance())
                    await asyncio.sleep(1800)
                    continue

                if (now.timestamp() - self.last_health_check) > self.health_check_interval:
                    await self._health_check()
                    self.last_health_check = now.timestamp()

                latest = await self.api.get_latest_result()
                if latest:
                    qihao = latest.get('qihao')
                    if qihao != self.last_qihao:
                        logger.log_game(f"检测到新期号: {qihao}")
                        if self.model.is_training:
                            logger.log_system("检测到新开奖，将停止训练")
                        await self._on_new_period(qihao, latest)
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.log_error(0, "全局调度器异常", e)
                await asyncio.sleep(10)

    async def _feed_keno_history(self):
        keno_list = list(self.api.keno_cache)
        kj_list = list(self.api.history_cache)
        if not keno_list or not kj_list:
            logger.log_system("Keno或开奖历史数据不足，无法预加载相似性模块")
            return
        for i in range(min(len(keno_list), len(kj_list))):
            keno_item = keno_list[i]
            kj_item = kj_list[i]
            self.model.keno_similarity.add_keno_data(keno_item, kj_item)
        logger.log_system(f"已加载 {min(len(keno_list), len(kj_list))} 条Keno历史数据到相似性模块")

    async def _health_check(self):
        now = datetime.now()
        expired_phones = []
        for phone, cache in self.account_manager.balance_cache.items():
            if (now - cache['time']).seconds > Config.BALANCE_CACHE_SECONDS * 2:
                expired_phones.append(phone)
        for phone in expired_phones:
            del self.account_manager.balance_cache[phone]

        if self.prediction_broadcaster.global_predictions.get('last_update'):
            last_update = datetime.fromisoformat(self.prediction_broadcaster.global_predictions['last_update'])
            if (now - last_update).total_seconds() > 86400:
                self.prediction_broadcaster.global_predictions['predictions'] = []
                self.prediction_broadcaster.global_predictions['last_open_qihao'] = None
                self.prediction_broadcaster.global_predictions['next_qihao'] = None

        for phone, acc in self.account_manager.accounts.items():
            if acc.is_logged_in:
                connected = await self.account_manager.ensure_client_connected(phone)
                if not connected:
                    logger.log_system(f"健康检查: 账户 {phone} 连接失效，已标记为未登录")

    async def _on_new_period(self, qihao, latest):
        try:
            if self.model.is_training:
                logger.log_system("检测到新开奖，将停止训练")

            for phone, acc in self.account_manager.accounts.items():
                if acc.last_bet_period and acc.last_bet_period != qihao:
                    self._create_task(
                        self.game_scheduler.check_bet_result(phone, acc.last_bet_period, latest)
                    )

            keno = await self.api.get_latest_keno()
            if keno:
                self.model.keno_similarity.add_keno_data(keno, latest)
                self.model.keno_similarity.current_keno_nbrs = keno['nbrs']
                self.model.current_keno_nbrs = keno['nbrs']

            history = await self.api.get_history(50)
            if len(history) < 3:
                logger.log_game("历史数据不足，跳过预测")
                return

            prediction = self.model.predict(history, latest)
            next_qihao = increment_qihao(qihao)

            await self.prediction_broadcaster.update_global_predictions(prediction, next_qihao, latest)

            for phone, acc in self.account_manager.accounts.items():
                if (acc.auto_betting and acc.is_logged_in and acc.game_group_id and
                        acc.last_bet_period != qihao):
                    delay = random.uniform(0, 3)
                    await asyncio.sleep(delay)
                    self._create_task(self._execute_bet_with_semaphore(phone, prediction, latest))

            self.last_qihao = qihao

        except Exception as e:
            logger.log_error(0, f"处理新期号 {qihao} 失败", e)

    async def _execute_bet_with_semaphore(self, phone, prediction, latest):
        async with self.bet_semaphore:
            await self.game_scheduler.execute_bet(phone, prediction, latest)

    async def _check_new_result(self):
        """供ModelManager调用的新开奖检测"""
        current = await self.api.get_latest_result()
        if current and current.get('qihao') != self.last_qihao:
            return True
        return False

# ==================== 主Bot类 ====================
class PC28Bot:
    def __init__(self):
        self.api = PC28API()
        self.account_manager = AccountManager()
        self.model = ModelManager()
        self.strategy_manager = BettingStrategyManager(self.account_manager)
        self.amount_manager = AmountManager(self.account_manager)
        self.game_scheduler = GameScheduler(self.account_manager, self.model, self.api)
        self.global_scheduler = GlobalScheduler(
            self.account_manager, self.model, self.api,
            None, self.game_scheduler
        )
        self.prediction_broadcaster = PredictionBroadcaster(self.account_manager, self.model, self.api, self.global_scheduler)
        self.global_scheduler.prediction_broadcaster = self.prediction_broadcaster

        self.application = Application.builder().token(Config.BOT_TOKEN).build()
        self._register_handlers()
        logger.log_system("PC28 Bot 初始化完成")

    def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("cancel", self.cmd_cancel))
        self.application.add_handler(CommandHandler("train", self.cmd_offline_train))
        self.application.add_handler(CommandHandler("resetmodel", self.cmd_reset_model))

        conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.login_select, pattern=r'^login_select:')],
            states={
                Config.LOGIN_SELECT: [],
                Config.LOGIN_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.login_code)],
                Config.LOGIN_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.login_password)],
            },
            fallbacks=[CommandHandler('cancel', self.cmd_cancel)],
        )
        self.application.add_handler(conv_handler)

        add_account_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_account_start, pattern=r'^add_account$')],
            states={
                Config.ADD_ACCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_account_input)],
            },
            fallbacks=[CommandHandler('cancel', self.cmd_cancel)],
        )
        self.application.add_handler(add_account_conv)

        chase_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.chase_start, pattern=r'^action:setchase:')],
            states={
                Config.CHASE_NUMBERS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.chase_input_numbers)],
                Config.CHASE_PERIODS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.chase_input_periods)],
                Config.CHASE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.chase_input_amount)],
            },
            fallbacks=[
                CommandHandler('cancel', self.cmd_cancel),
                CallbackQueryHandler(self.chase_cancel, pattern=r'^chase_cancel:')
            ],
        )
        self.application.add_handler(chase_conv)

        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text_message))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        self.application.add_error_handler(self.error_handler)

        self.application.add_handler(CallbackQueryHandler(self.amount_recommend, pattern=r'^amount_recommend:'))
        self.application.add_handler(CallbackQueryHandler(self.amount_set_confirm, pattern=r'^amount_set_confirm:'))

    async def error_handler(self, update, context):
        logger.log_error(0, "Bot错误", str(context.error))

    async def cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("✅ 操作已取消")
        return ConversationHandler.END

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [InlineKeyboardButton("📱 账户管理", callback_data="menu:accounts")],
            [InlineKeyboardButton("🎯 智能预测", callback_data="menu:prediction")],
            [InlineKeyboardButton("📊 系统状态", callback_data="menu:status")],
            [InlineKeyboardButton("❓ 帮助", callback_data="menu:help")],
            [InlineKeyboardButton("📖 使用手册", url=Config.MANUAL_LINK)]
        ]
        await update.message.reply_text(
            "🎰 *PC28 智能预测投注系统*\n\n"
            "✨ 欢迎使用！请选择操作：",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    async def cmd_offline_train(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in Config.ADMIN_USER_IDS:
            await update.message.reply_text("❌ 权限不足")
            return

        await update.message.reply_text("🔄 开始离线训练，这可能需要几分钟...")

        try:
            kj_url = f"https://www.pc28.ai/api/history/kj.csv?nbr={Config.KJ_HISTORY_DOWNLOAD}"
            kj_rows = await self.api.download_csv_data(kj_url)
            kj_data = []
            for row in kj_rows:
                parsed = self.api._parse_kj_csv_row(row)
                if parsed:
                    kj_data.append(parsed)

            keno_url = f"https://www.pc28.ai/api/history/keno.csv?nbr={Config.KENO_HISTORY_DOWNLOAD}"
            keno_rows = await self.api.download_csv_data(keno_url)
            keno_data = []
            for row in keno_rows:
                parsed = self.api._parse_keno_csv_row(row)
                if parsed:
                    keno_data.append(parsed)

            if kj_data and keno_data:
                await self.model.offline_train(kj_data, keno_data, max_epochs=Config.TRAIN_EPOCHS)
                await update.message.reply_text("✅ 离线训练完成！")
            else:
                await update.message.reply_text("❌ 下载数据失败")
        except Exception as e:
            await update.message.reply_text(f"❌ 训练失败: {str(e)[:100]}")

    async def cmd_reset_model(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in Config.ADMIN_USER_IDS:
            await update.message.reply_text("❌ 权限不足")
            return
        self.model.reset_weights()
        await update.message.reply_text("✅ 模型权重已重置为初始值")

    async def add_account_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        context.user_data['adding_account'] = True
        await query.edit_message_text("📱 请输入手机号（包含国际区号，如 +861234567890）：\n\n点击 /cancel 取消")
        return Config.ADD_ACCOUNT

    async def add_account_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        phone = update.message.text.strip()
        ok, msg = await self.account_manager.add_account(user_id, phone)
        if ok:
            await update.message.reply_text(f"✅ {msg}")
            await self._show_account_detail(update.message, user_id, phone)
        else:
            await update.message.reply_text(f"❌ {msg}")
            await self._show_main_menu(update.message)
        return ConversationHandler.END

    async def login_select(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        phone = query.data.split(':')[1]
        context.user_data['login_phone'] = phone
        context.user_data['login_user_id'] = query.from_user.id
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("账户不存在")
            return ConversationHandler.END
        if acc.is_logged_in:
            await self._show_account_detail(query, query.from_user.id, phone)
            return ConversationHandler.END

        client = self.account_manager.create_client(phone)
        if not client:
            await query.edit_message_text("创建客户端失败")
            return ConversationHandler.END

        try:
            await client.connect()
            if await client.is_user_authorized():
                me = await client.get_me()
                display = f"{me.first_name or ''} {me.last_name or ''}".strip()
                await self.account_manager.update_account(phone,
                    is_logged_in=True,
                    display_name=display,
                    telegram_user_id=me.id
                )
                await self._show_account_detail(query, query.from_user.id, phone)
                return ConversationHandler.END
            else:
                res = await client.send_code_request(phone)
                self.account_manager.set_login_session(phone, {'phone_code_hash': res.phone_code_hash})
                await query.edit_message_text(f"📨 验证码已发送到 `{phone}`\n\n请输入验证码：", parse_mode='Markdown')
                return Config.LOGIN_CODE
        except Exception as e:
            logger.log_error(query.from_user.id, f"登录失败 {phone}", e)
            await query.edit_message_text(f"❌ 登录失败：{str(e)[:200]}")
            return ConversationHandler.END

    async def login_code(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user.id
        phone = context.user_data.get('login_phone')
        if not phone:
            await update.message.reply_text("登录会话已过期，请重新开始")
            return ConversationHandler.END
        code = update.message.text.strip()
        acc = self.account_manager.get_account(phone)
        if not acc:
            await update.message.reply_text("账户不存在")
            return ConversationHandler.END
        client = self.account_manager.clients.get(phone)
        if not client:
            await update.message.reply_text("客户端丢失")
            return ConversationHandler.END
        sess = self.account_manager.get_login_session(phone)
        if not sess:
            await update.message.reply_text("登录会话已过期，请重新开始")
            return ConversationHandler.END
        try:
            await client.sign_in(phone, code, phone_code_hash=sess['phone_code_hash'])
            me = await client.get_me()
            display = f"{me.first_name or ''} {me.last_name or ''}".strip()
            await self.account_manager.update_account(phone,
                is_logged_in=True,
                needs_2fa=False,
                display_name=display,
                telegram_user_id=me.id
            )
            self.account_manager.login_sessions.pop(phone, None)
            await self._show_account_detail(update.message, user, phone)
            return ConversationHandler.END
        except SessionPasswordNeededError:
            await self.account_manager.update_account(phone, needs_2fa=True)
            if hasattr(client, '_phone_code_hash'):
                acc.login_temp_data['phone_code_hash'] = client._phone_code_hash
            await update.message.reply_text("🔒 此账户启用了两步验证，请输入密码：")
            return Config.LOGIN_PASSWORD
        except PhoneCodeExpiredError:
            await update.message.reply_text("❌ 验证码已过期，请重新获取。")
            # 重新发送验证码
            try:
                res = await client.send_code_request(phone)
                self.account_manager.set_login_session(phone, {'phone_code_hash': res.phone_code_hash})
                await update.message.reply_text(f"📨 新的验证码已发送到 `{phone}`\n\n请输入验证码：", parse_mode='Markdown')
                return Config.LOGIN_CODE
            except Exception as e:
                logger.log_error(user, f"重新发送验证码失败 {phone}", e)
                await update.message.reply_text("❌ 重新发送验证码失败，请稍后重试。")
                return ConversationHandler.END
        except Exception as e:
            logger.log_error(user, f"验证码验证失败 {phone}", e)
            error_msg = str(e)
            if "invalid" in error_msg.lower():
                error_msg = "验证码无效，请重新输入"
            await update.message.reply_text(f"❌ 验证失败：{error_msg}\n请重新输入验证码或点击 /cancel 取消")
            return Config.LOGIN_CODE

    async def login_password(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user.id
        phone = context.user_data.get('login_phone')
        if not phone:
            await update.message.reply_text("登录会话已过期，请重新开始")
            return ConversationHandler.END
        pwd = update.message.text.strip()
        acc = self.account_manager.get_account(phone)
        if not acc:
            await update.message.reply_text("账户不存在")
            return ConversationHandler.END
        client = self.account_manager.clients.get(phone)
        if not client:
            await update.message.reply_text("客户端丢失")
            return ConversationHandler.END
        try:
            await client.sign_in(password=pwd)
            me = await client.get_me()
            display = f"{me.first_name or ''} {me.last_name or ''}".strip()
            await self.account_manager.update_account(phone,
                is_logged_in=True,
                needs_2fa=False,
                display_name=display,
                telegram_user_id=me.id
            )
            acc.login_temp_data = {}
            await self._show_account_detail(update.message, user, phone)
            return ConversationHandler.END
        except Exception as e:
            logger.log_error(user, f"密码验证失败 {phone}", e)
            error_msg = str(e)
            if "invalid" in error_msg.lower():
                error_msg = "密码无效，请重新输入"
            await update.message.reply_text(f"❌ 密码验证失败：{error_msg}\n请重新输入密码或点击 /cancel 取消")
            return Config.LOGIN_PASSWORD

    async def chase_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        phone = query.data.split(':')[1]
        context.user_data['chase_phone'] = phone
        context.user_data['chase_step'] = 'numbers'

        text = (
            "🔢 *设置数字追号 - 第1步/共3步*\n\n"
            "请输入要追的数字（0-27），多个数字可用空格、逗号或顿号分隔。\n"
            "例如：`0 5 12` 或 `0,5,12` 或 `0、5、12`\n\n"
            "📌 说明：追号将每期自动投注您指定的所有数字，直到期数用完或手动停止。"
        )
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ 取消", callback_data=f"chase_cancel:{phone}")]
        ])
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        return Config.CHASE_NUMBERS

    async def chase_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        phone = query.data.split(':')[1]

        context.user_data.clear()

        await self._show_account_detail(query, query.from_user.id, phone)
        return ConversationHandler.END

    async def chase_input_numbers(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        parts = re.split(r'[,\s、]+', text)
        numbers = []
        for p in parts:
            p = p.strip()
            if p.lstrip('-').isdigit():
                num = int(p)
                if 0 <= num <= 27:
                    numbers.append(num)
        numbers = list(set(numbers))

        if not numbers:
            phone = context.user_data.get('chase_phone')
            await update.message.reply_text(
                "❌ 未输入有效数字（0-27），请重新输入：",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ 取消", callback_data=f"chase_cancel:{phone}")]
                ])
            )
            return Config.CHASE_NUMBERS

        context.user_data['chase_numbers'] = numbers
        phone = context.user_data['chase_phone']

        text = (
            f"✅ 已记录数字：{', '.join(map(str, numbers))}\n\n"
            "🔢 *第2步/共3步：请输入追号期数*\n\n"
            "请输入一个正整数，表示要连续追多少期。\n"
            "例如：`10` 表示连续追10期。"
        )
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ 取消", callback_data=f"chase_cancel:{phone}")]
        ])
        await update.message.reply_text(text, reply_markup=reply_markup)
        return Config.CHASE_PERIODS

    async def chase_input_periods(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        if not text.isdigit() or int(text) <= 0:
            phone = context.user_data.get('chase_phone')
            await update.message.reply_text(
                "❌ 期数必须是正整数，请重新输入：",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ 取消", callback_data=f"chase_cancel:{phone}")]
                ])
            )
            return Config.CHASE_PERIODS

        periods = int(text)
        context.user_data['chase_periods'] = periods
        phone = context.user_data['chase_phone']

        text = (
            f"✅ 已设置期数：{periods} 期\n\n"
            "🔢 *第3步/共3步：请输入每注金额*\n\n"
            "请输入一个整数（单位：KK）。\n"
            "• 如果输入 `0`，则使用当前账户的基础金额。\n"
            "• 金额不能超过账户最大金额限制。\n"
            "例如：`1000` 表示每注1000KK。"
        )
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ 取消", callback_data=f"chase_cancel:{phone}")]
        ])
        await update.message.reply_text(text, reply_markup=reply_markup)
        return Config.CHASE_AMOUNT

    async def chase_input_amount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        try:
            amount = int(text)
        except ValueError:
            phone = context.user_data.get('chase_phone')
            await update.message.reply_text(
                "❌ 金额必须是整数，请重新输入：",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ 取消", callback_data=f"chase_cancel:{phone}")]
                ])
            )
            return Config.CHASE_AMOUNT

        if amount < 0:
            phone = context.user_data.get('chase_phone')
            await update.message.reply_text(
                "❌ 金额不能为负数，请重新输入：",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ 取消", callback_data=f"chase_cancel:{phone}")]
                ])
            )
            return Config.CHASE_AMOUNT

        phone = context.user_data['chase_phone']
        numbers = context.user_data['chase_numbers']
        periods = context.user_data['chase_periods']

        await self.account_manager.update_account(
            phone,
            chase_enabled=True,
            chase_numbers=numbers,
            chase_periods=periods,
            chase_current=0,
            chase_amount=amount,
            chase_stop_reason=None
        )

        user_id = update.effective_user.id
        self.account_manager.set_user_state(user_id, 'account_selected', {'current_account': phone})

        context.user_data.clear()

        await update.message.reply_text(
            f"✅ *追号设置成功！*\n\n"
            f"📌 数字：{', '.join(map(str, numbers))}\n"
            f"📌 期数：{periods}\n"
            f"📌 每注金额：{amount if amount>0 else '使用基础金额'}KK\n\n"
            f"🔍 您可以在账户详情页查看追号状态。"
        )

        acc = self.account_manager.get_account(phone)
        if acc:
            await self._show_account_detail(update.message, user_id, phone)
        else:
            await self._show_accounts_menu(update.message, user_id)

        return ConversationHandler.END

    async def handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user.id
        state = self.account_manager.get_user_state(user)
        phone = state.get('current_account')
        if not phone:
            return
        acc = self.account_manager.get_account(phone)
        if not acc:
            return

        if context.user_data.get('login_phone'):
            return

        input_mode = acc.input_mode
        if input_mode and input_mode in ['base_amount', 'max_amount', 'stop_balance', 'stop_loss', 'stop_win', 'resume_balance']:
            try:
                amount = int(update.message.text.strip())
            except ValueError:
                await update.message.reply_text("❌ 请输入整数金额")
                return
            ok, msg = await self.amount_manager.set_param(phone, input_mode, amount, user)
            if ok:
                await self.account_manager.update_account(phone, input_mode=None, input_buffer='')
                await update.message.reply_text(f"✅ {msg}")
                await self._show_account_detail(update.message, user, phone)
            else:
                await update.message.reply_text(f"❌ {msg}")
        else:
            pass

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        user = query.from_user.id
        logger.log_account(user, "", f"回调数据: {data}")

        # 优先处理特定回调
        if data.startswith('amount_recommend:'):
            await self.amount_recommend(update, context)
            return
        if data.startswith('amount_set_confirm:'):
            await self.amount_set_confirm(update, context)
            return
        if data.startswith('login_select:'):
            await self.login_select(update, context)
            return

        route_map = {
            "menu:main": self._show_main_menu,
            "menu:prediction": self._show_prediction_menu,
            "menu:status": self._show_status_menu,
            "menu:help": self._show_help_menu,
            "add_account": self.add_account_start,
            "run_analysis": self._process_run_analysis,
            "refresh_status": self._show_status_menu,
        }
        if data in route_map:
            await route_map[data](query)
            return

        if data == "menu:accounts":
            await self._show_accounts_menu(query, user)
            return

        if data.startswith("select_account:"):
            phone = data.split(":")[1]
            await self._show_account_detail(query, user, phone)
        elif data.startswith("action:"):
            parts = data.split(":")
            action = parts[1]
            phone = parts[2] if len(parts) > 2 else None
            await self._process_action(query, user, action, phone)
        elif data.startswith("amount_menu:"):
            phone = data.split(":")[1]
            await self._show_amount_menu_callback(query, user, phone)
        elif data.startswith("amount_set:"):
            parts = data.split(":")
            param_name = parts[1]
            phone = parts[2]
            await self._amount_set_callback(query, user, phone, param_name)
        elif data.startswith("set_strategy:"):
            parts = data.split(":")
            if len(parts) == 3:
                phone = parts[1]
                strategy = parts[2]
                await self._process_set_strategy(query, user, phone, strategy)
        elif data.startswith("set_scheme:"):
            parts = data.split(":")
            if len(parts) == 3:
                phone = parts[1]
                scheme = parts[2]
                await self._process_set_scheme(query, user, phone, scheme)
        elif data.startswith("set_group:"):
            group_id = int(data.split(":")[1])
            await self._set_group_callback(query, user, group_id)
        elif data.startswith("set_pred_group:"):
            group_id = int(data.split(":")[1])
            await self._set_pred_group_callback(query, user, group_id)
        elif data.startswith("toggle_content:"):
            phone = data.split(":")[1]
            await self._toggle_prediction_content(query, user, phone)
        elif data.startswith("clear_streak:"):
            phone = data.split(":")[1]
            await self._clear_streak_records(query, user, phone)
        else:
            logger.log_error(user, "未知回调", data)

    async def _show_main_menu(self, query):
        kb = [
            [InlineKeyboardButton("📱 账户管理", callback_data="menu:accounts")],
            [InlineKeyboardButton("🎯 智能预测", callback_data="menu:prediction")],
            [InlineKeyboardButton("📊 系统状态", callback_data="menu:status")],
            [InlineKeyboardButton("❓ 帮助", callback_data="menu:help")],
            [InlineKeyboardButton("📖 使用手册", url=Config.MANUAL_LINK)]
        ]
        text = "🎮 *PC28 智能投注系统*\n\n请选择操作："
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_accounts_menu(self, query, user):
        accounts = self.account_manager.get_user_accounts(user)
        kb = []
        if not accounts:
            text = "📭 您还没有添加账户"
        else:
            text = "📱 *您的账户列表*\n\n"
            for acc in accounts:
                status = "✅" if acc.is_logged_in else "❌"
                text += f"{status} {acc.get_display_name()}\n"
        kb.append([InlineKeyboardButton("➕ 添加账户", callback_data="add_account")])
        if accounts:
            for acc in accounts:
                kb.append([InlineKeyboardButton(f"{acc.get_display_name()}", callback_data=f"select_account:{acc.phone}")])
        kb.append([InlineKeyboardButton("🔙 返回", callback_data="menu:main")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_account_detail(self, query_or_message, user, phone):
        self.account_manager.set_user_state(user, 'account_selected', {'current_account': phone})
        acc = self.account_manager.get_account(phone)
        if not acc:
            if hasattr(query_or_message, 'edit_message_text'):
                try:
                    await query_or_message.edit_message_text("❌ 账户不存在，返回账户列表")
                except BadRequest:
                    pass
                await self._show_accounts_menu(query_or_message, user)
            else:
                await query_or_message.reply_text("❌ 账户不存在，返回账户列表")
            return

        display = acc.get_display_name()
        status = "✅ 已登录" if acc.is_logged_in else "❌ 未登录"
        if acc.auto_betting: status += " | 🤖 自动投注"
        if acc.prediction_broadcast: status += " | 📊 播报中"
        if acc.broadcast_stop_requested: status += " | ⏳ 停止中"
        if acc.recommend_mode: status += f" | 💰 推荐({acc.risk_profile})"

        bet_button = "🛑 停止自动投注" if acc.auto_betting else "🤖 开启自动投注"
        pred_button = "🛑 停止播报" if acc.prediction_broadcast else "📊 开启播报"
        if acc.broadcast_stop_requested:
            pred_button = "⏳ 停止请求中"

        net_profit = acc.total_profit - acc.total_loss

        betting_menu = [
            [InlineKeyboardButton("🎯 投注方案", callback_data=f"action:setscheme:{phone}"),
             InlineKeyboardButton("📈 金额策略", callback_data=f"action:setstrategy:{phone}")],
            [InlineKeyboardButton("💰 设置金额", callback_data=f"amount_menu:{phone}"),
             InlineKeyboardButton("🔢 设置追号", callback_data=f"action:setchase:{phone}")],
        ]

        content_type = "双组" if acc.prediction_content == "double" else "杀组"
        broadcast_menu = [
            [InlineKeyboardButton("📢 播报群", callback_data=f"action:listpredgroups:{phone}"),
             InlineKeyboardButton(f"🎛️ 播报内容({content_type})", callback_data=f"toggle_content:{phone}")],
        ]

        kb = [
            [InlineKeyboardButton("🔐 登录", callback_data=f"login_select:{phone}"),
             InlineKeyboardButton("🚪 登出", callback_data=f"action:logout:{phone}")],
            [InlineKeyboardButton("💬 游戏群", callback_data=f"action:listgroups:{phone}")],
        ] + betting_menu + broadcast_menu + [
            [InlineKeyboardButton(bet_button, callback_data=f"action:toggle_bet:{phone}"),
             InlineKeyboardButton(pred_button, callback_data=f"action:toggle_pred:{phone}")],
            [InlineKeyboardButton("💰 查询余额", callback_data=f"action:balance:{phone}"),
             InlineKeyboardButton("📊 账户状态", callback_data=f"action:status:{phone}")],
            [InlineKeyboardButton("📊 连输连赢记录(双组)", callback_data=f"action:streak_double:{phone}"),
             InlineKeyboardButton("📊 连输连赢记录(杀组)", callback_data=f"action:streak_kill:{phone}")],
            [InlineKeyboardButton("📚 手动投注说明", callback_data=f"action:manual_bet_help:{phone}")],
            [InlineKeyboardButton("🔙 返回", callback_data="menu:accounts")]
        ]

        if acc.chase_enabled:
            status += f" | 🔢 追{acc.chase_current}/{acc.chase_periods}"
            kb.insert(4, [InlineKeyboardButton("🛑 停止追号", callback_data=f"action:stopchase:{phone}")])

        base_amount_text = "一键推荐模式(动态)" if acc.recommend_mode else f"{acc.bet_params.base_amount} KK"

        text = f"📱 *账户: {display}*\n\n状态: {status}\n净盈利: {net_profit:.0f}K\n基础金额: {base_amount_text}\n\n选择操作:"

        if hasattr(query_or_message, 'edit_message_text'):
            try:
                await query_or_message.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    raise e
        else:
            await query_or_message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _toggle_prediction_content(self, query, user, phone):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return
        new_content = "kill" if acc.prediction_content == "double" else "double"
        await self.account_manager.update_account(phone, prediction_content=new_content)
        await query.edit_message_text(f"✅ 播报内容已切换为 {'杀组' if new_content=='kill' else '双组'}")
        await self._show_account_detail(query, user, phone)

    async def _show_prediction_menu(self, query):
        kb = [
            [InlineKeyboardButton("🔮 运行预测", callback_data="run_analysis")],
            [InlineKeyboardButton("🔙 返回", callback_data="menu:main")]
        ]
        await query.edit_message_text("🎯 *预测分析菜单*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_amount_menu_callback(self, query, user, phone):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return

        base_amount_text = "一键推荐模式(动态)" if acc.recommend_mode else f"{acc.bet_params.base_amount} KK"
        recommend_button_text = "🛑 停止推荐" if acc.recommend_mode else "🤖 一键推荐"

        text = f"""
💰 *金额设置*

📱 账户: {acc.get_display_name()}

当前设置:
• 基础金额: {base_amount_text}
• 最大金额: {acc.bet_params.max_amount} KK
• 停止余额: {acc.bet_params.stop_balance} KK
• 止损金额: {acc.bet_params.stop_loss} KK
• 止盈金额: {acc.bet_params.stop_win} KK
• 恢复余额: {acc.bet_params.resume_balance} KK

请选择需要修改的金额类型：
        """
        kb = [
            [InlineKeyboardButton("💰 基础金额", callback_data=f"amount_set:base_amount:{phone}"),
             InlineKeyboardButton("💎 最大金额", callback_data=f"amount_set:max_amount:{phone}")],
            [InlineKeyboardButton("🛑 停止余额", callback_data=f"amount_set:stop_balance:{phone}"),
             InlineKeyboardButton("⛔ 止损金额", callback_data=f"amount_set:stop_loss:{phone}")],
            [InlineKeyboardButton("✅ 止盈金额", callback_data=f"amount_set:stop_win:{phone}"),
             InlineKeyboardButton("🔄 恢复余额", callback_data=f"amount_set:resume_balance:{phone}")],
            [InlineKeyboardButton(recommend_button_text, callback_data=f"amount_recommend:{phone}")],
            [InlineKeyboardButton("🔙 返回账户详情", callback_data=f"select_account:{phone}")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _amount_set_callback(self, query, user, phone, param_name):
        param_names = {
            'base_amount': '基础金额',
            'max_amount': '最大金额',
            'stop_balance': '停止余额',
            'stop_loss': '止损金额',
            'stop_win': '止盈金额',
            'resume_balance': '恢复余额'
        }
        display_name = param_names.get(param_name, param_name)
        await self.account_manager.update_account(phone, input_mode=param_name, input_buffer='')
        text = f"🔢 请输入新的 {display_name}（整数KK）："
        kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"amount_menu:{phone}")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def amount_recommend(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        phone = query.data.split(':')[1]
        user = query.from_user.id

        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return

        if acc.recommend_mode:
            # 如果已经是推荐模式，点击则退出
            await self.account_manager.update_account(phone, recommend_mode=False)
            await query.edit_message_text("✅ 已退出推荐模式，恢复手动设置")
            await self._show_amount_menu_callback(query, user, phone)
            return

        # 否则询问是否启用推荐模式（不再计算推荐金额，因为会自动根据余额动态调整）
        keyboard = [
            [InlineKeyboardButton("✅ 确认启用推荐模式", callback_data=f"amount_set_confirm:{phone}:0")],  # 金额参数保留但未使用
            [InlineKeyboardButton("❌ 取消", callback_data=f"amount_menu:{phone}")]
        ]
        await query.edit_message_text(
            "启用推荐模式后，系统将根据当前余额和风险偏好（在金额策略中设置）自动计算每期投注金额。\n\n是否确认启用？",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def amount_set_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        _, phone, _ = query.data.split(':')  # 忽略金额参数
        user = query.from_user.id

        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return

        await self.account_manager.update_account(phone, recommend_mode=True)
        await query.edit_message_text("✅ 已启用推荐模式，投注金额将根据余额动态调整。\n您可以通过“金额策略”按钮调整风险偏好（保守/稳定/激进）。")
        await self._show_amount_menu_callback(query, user, phone)

    async def _process_action(self, query, user, action, phone):
        if action == "logout":
            await self._cmd_logout_inline(query, user, phone)
        elif action == "toggle_bet":
            acc = self.account_manager.get_account(phone)
            if acc.auto_betting:
                await self.game_scheduler.stop_auto_betting(phone, user)
            else:
                await self.game_scheduler.start_auto_betting(phone, user)
            await self._show_account_detail(query, user, phone)
        elif action == "toggle_pred":
            acc = self.account_manager.get_account(phone)
            if acc.prediction_broadcast:
                await self.prediction_broadcaster.stop_broadcast(phone, user)
            else:
                await self.prediction_broadcaster.start_broadcast(phone, user)
            await self._show_account_detail(query, user, phone)
        elif action == "balance":
            bal = await self.game_scheduler.get_balance(phone)
            if bal is not None:
                text = f"💰 余额: {bal:.2f} KK"
            else:
                text = "❌ 查询失败"
            kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        elif action == "status":
            await self._show_account_status(query, phone)
        elif action == "streak_double":
            await self._show_streak_records(query, phone, content_type="double")
        elif action == "streak_kill":
            await self._show_streak_records(query, phone, content_type="kill")
        elif action == "setstrategy":
            await self._show_strategy_selection(query, phone)
        elif action == "setscheme":
            await self._show_scheme_selection(query, phone)
        elif action == "listgroups":
            await self._list_groups_for_selection(query, phone)
        elif action == "listpredgroups":
            await self._list_groups_for_prediction(query, phone)
        elif action == "manual_bet_help":
            text = """
📚 *手动投注说明*

您可以在游戏群直接发送消息进行投注，例如：
`大 10000`
`小单 5000`

支持的类型：大、小、单、双、大单、大双、小单、小双

注意：请在封盘前完成投注。
            """
            kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        elif action == "stopchase":
            await self.account_manager.update_account(phone,
                chase_enabled=False,
                chase_stop_reason="手动停止",
                chase_numbers=[],
                chase_periods=0,
                chase_current=0,
                chase_amount=0
            )
            await self._show_account_detail(query, user, phone)
        else:
            await query.edit_message_text("❌ 未知操作", parse_mode='Markdown')

    async def _list_groups_for_selection(self, query, phone):
        client = self.account_manager.clients.get(phone)
        if not client:
            await query.edit_message_text("❌ 客户端未连接", parse_mode='Markdown')
            return
        try:
            dialogs = await client.get_dialogs(limit=30)
            groups = [d for d in dialogs if d.is_group or d.is_channel]
            if not groups:
                kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]]
                await query.edit_message_text("📭 未找到任何群组", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
                return
            kb = []
            for g in groups[:10]:
                icon = "📢" if g.is_channel else "👥"
                kb.append([InlineKeyboardButton(f"{icon} {g.name[:30]}", callback_data=f"set_group:{g.id}")])
            kb.append([InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")])
            await query.edit_message_text("📋 *选择游戏群:*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception as e:
            logger.log_error(0, f"获取群组列表失败 {phone}", e)
            kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]]
            await query.edit_message_text("❌ 获取群组列表失败", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _list_groups_for_prediction(self, query, phone):
        client = self.account_manager.clients.get(phone)
        if not client:
            await query.edit_message_text("❌ 客户端未连接", parse_mode='Markdown')
            return
        try:
            dialogs = await client.get_dialogs(limit=30)
            groups = [d for d in dialogs if d.is_group or d.is_channel]
            if not groups:
                kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]]
                await query.edit_message_text("📭 未找到任何群组", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
                return
            kb = []
            for g in groups[:10]:
                icon = "📢" if g.is_channel else "👥"
                kb.append([InlineKeyboardButton(f"{icon} {g.name[:30]}", callback_data=f"set_pred_group:{g.id}")])
            kb.append([InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")])
            await query.edit_message_text("📋 *选择预测播报群:*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        except Exception as e:
            logger.log_error(0, f"获取群组列表失败 {phone}", e)
            kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]]
            await query.edit_message_text("❌ 获取群组列表失败", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _set_group_callback(self, query, user, group_id):
        phone = self.account_manager.get_user_state(user).get('current_account')
        if not phone:
            await query.edit_message_text("❌ 请先选择账户")
            return
        client = self.account_manager.clients.get(phone)
        group_name = str(group_id)
        if client:
            try:
                entity = await client.get_entity(group_id)
                group_name = getattr(entity, 'title', str(group_id))
            except:
                pass
        await self.account_manager.update_account(phone, game_group_id=group_id, game_group_name=group_name)
        await self._show_account_detail(query, user, phone)

    async def _set_pred_group_callback(self, query, user, group_id):
        phone = self.account_manager.get_user_state(user).get('current_account')
        if not phone:
            await query.edit_message_text("❌ 请先选择账户")
            return
        client = self.account_manager.clients.get(phone)
        group_name = str(group_id)
        if client:
            try:
                entity = await client.get_entity(group_id)
                group_name = getattr(entity, 'title', str(group_id))
            except:
                pass
        await self.account_manager.update_account(phone, prediction_group_id=group_id, prediction_group_name=group_name)
        await self._show_account_detail(query, user, phone)

    async def _show_strategy_selection(self, query, phone):
        strategies = self.strategy_manager.strategies
        kb = []
        for name in strategies.keys():
            kb.append([InlineKeyboardButton(name, callback_data=f"set_strategy:{phone}:{name}")])
        kb.append([InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")])
        await query.edit_message_text("📊 *选择投注策略:*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_scheme_selection(self, query, phone):
        schemes = self.strategy_manager.schemes
        kb = []
        for name in schemes.keys():
            kb.append([InlineKeyboardButton(name, callback_data=f"set_scheme:{phone}:{name}")])
        kb.append([InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")])
        await query.edit_message_text("🎯 *选择投注方案:*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _process_set_strategy(self, query, user, phone, strategy):
        ok, msg = await self.strategy_manager.set_strategy(phone, strategy, user)
        if ok:
            await self._show_account_detail(query, user, phone)
        else:
            await query.edit_message_text(f"❌ {msg}", parse_mode='Markdown')

    async def _process_set_scheme(self, query, user, phone, scheme):
        ok, msg = await self.strategy_manager.set_scheme(phone, scheme, user)
        if ok:
            await self._show_account_detail(query, user, phone)
        else:
            await query.edit_message_text(f"❌ {msg}", parse_mode='Markdown')

    async def _show_account_status(self, query, phone):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return
        params = acc.bet_params
        total_profit = acc.total_profit
        total_loss = acc.total_loss
        net = total_profit - total_loss
        base_amount_text = "一键推荐模式(动态)" if acc.recommend_mode else f"{params.base_amount} KK"

        status = f"""
📱 *账户状态*

*基本信息:*
• 手机号: `{acc.phone}`
• 昵称: {acc.display_name or '无'}
• 登录状态: {'✅ 已登录' if acc.is_logged_in else '❌ 未登录'}

*监听状态:*
• 自动投注: {'✅ 开启' if acc.auto_betting else '❌ 关闭'}
• 预测播报: {'✅ 开启' if acc.prediction_broadcast else '❌ 关闭'}
• 播报内容: {'双组' if acc.prediction_content=='double' else '杀组'}

*投注设置:*
• 策略: {acc.betting_strategy}
• 方案: {acc.betting_scheme}
• 基础金额: {base_amount_text}
• 最大金额: {params.max_amount}KK
• 停止余额: {params.stop_balance}KK
• 止损: {params.stop_loss}KK
• 止盈: {params.stop_win}KK
• 恢复余额: {params.resume_balance}KK
"""
        if acc.chase_enabled:
            status += f"""
*追号状态:*
• 数字: {', '.join(map(str, acc.chase_numbers))}
• 进度: {acc.chase_current}/{acc.chase_periods}
• 每注金额: {acc.chase_amount if acc.chase_amount>0 else '使用基础'}KK
"""
        status += f"""
*统计:*
• 余额: {acc.balance:.2f}KK
• 总盈利: {total_profit:.2f}KK
• 总亏损: {total_loss:.2f}KK
• 净盈利: {net:.2f}KK
• 连赢: {acc.consecutive_wins} 连输: {acc.consecutive_losses}
        """
        kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]]
        await query.edit_message_text(status, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_streak_records(self, query, phone, content_type):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return

        if content_type == "double":
            records = acc.streak_records_double
            title = "双组"
        else:
            records = acc.streak_records_kill
            title = "杀组"

        if not records:
            await query.edit_message_text(
                f"📭 暂无{title}连输连赢记录\n\n当有7、8、10期连输或连赢时，会自动记录。",
                parse_mode='Markdown'
            )
            return

        text = f"📊 *{title}连输连赢记录 - {acc.get_display_name()}*\n\n"

        win_7 = [r for r in records if r.get('type') == 'win' and r.get('count') == 7]
        win_8 = [r for r in records if r.get('type') == 'win' and r.get('count') == 8]
        win_10 = [r for r in records if r.get('type') == 'win' and r.get('count') == 10]
        loss_7 = [r for r in records if r.get('type') == 'loss' and r.get('count') == 7]
        loss_8 = [r for r in records if r.get('type') == 'loss' and r.get('count') == 8]
        loss_10 = [r for r in records if r.get('type') == 'loss' and r.get('count') == 10]

        if win_7:
            text += f"\n✅ *连赢7期* ({len(win_7)}次)\n"
            for i, r in enumerate(win_7[-5:], 1):
                start = datetime.fromisoformat(r['start_time']).strftime('%m-%d %H:%M')
                end = datetime.fromisoformat(r['end_time']).strftime('%m-%d %H:%M')
                text += f"  {i}. {start} → {end}\n"

        if win_8:
            text += f"\n✅ *连赢8期* ({len(win_8)}次)\n"
            for i, r in enumerate(win_8[-5:], 1):
                start = datetime.fromisoformat(r['start_time']).strftime('%m-%d %H:%M')
                end = datetime.fromisoformat(r['end_time']).strftime('%m-%d %H:%M')
                text += f"  {i}. {start} → {end}\n"

        if win_10:
            text += f"\n✅ *连赢10期* ({len(win_10)}次)\n"
            for i, r in enumerate(win_10[-5:], 1):
                start = datetime.fromisoformat(r['start_time']).strftime('%m-%d %H:%M')
                end = datetime.fromisoformat(r['end_time']).strftime('%m-%d %H:%M')
                text += f"  {i}. {start} → {end}\n"

        if loss_7:
            text += f"\n❌ *连输7期* ({len(loss_7)}次)\n"
            for i, r in enumerate(loss_7[-5:], 1):
                start = datetime.fromisoformat(r['start_time']).strftime('%m-%d %H:%M')
                end = datetime.fromisoformat(r['end_time']).strftime('%m-%d %H:%M')
                text += f"  {i}. {start} → {end}\n"

        if loss_8:
            text += f"\n❌ *连输8期* ({len(loss_8)}次)\n"
            for i, r in enumerate(loss_8[-5:], 1):
                start = datetime.fromisoformat(r['start_time']).strftime('%m-%d %H:%M')
                end = datetime.fromisoformat(r['end_time']).strftime('%m-%d %H:%M')
                text += f"  {i}. {start} → {end}\n"

        if loss_10:
            text += f"\n❌ *连输10期* ({len(loss_10)}次)\n"
            for i, r in enumerate(loss_10[-5:], 1):
                start = datetime.fromisoformat(r['start_time']).strftime('%m-%d %H:%M')
                end = datetime.fromisoformat(r['end_time']).strftime('%m-%d %H:%M')
                text += f"  {i}. {start} → {end}\n"

        if not (win_7 or win_8 or win_10 or loss_7 or loss_8 or loss_10):
            text = f"📊 *{title}连输连赢记录 - {acc.get_display_name()}*\n\n暂无记录"

        kb = [
            [InlineKeyboardButton("🗑️ 删除所有记录", callback_data=f"clear_streak:{phone}")],
            [InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _clear_streak_records(self, query, user, phone):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return
        await self.account_manager.update_account(phone, streak_records_double=[], streak_records_kill=[])
        await query.edit_message_text("✅ 所有连输连赢记录已删除")
        await self._show_account_detail(query, user, phone)

    async def _show_status_menu(self, query):
        api_stats = self.api.get_statistics()
        sched_stats = self.game_scheduler.get_stats()
        total_accounts = len(self.account_manager.accounts)
        logged = sum(1 for a in self.account_manager.accounts.values() if a.is_logged_in)
        auto = sched_stats['auto_betting_accounts']
        broadcast = sched_stats['broadcast_accounts']
        total_profit = sum(a.total_profit for a in self.account_manager.accounts.values())
        total_loss = sum(a.total_loss for a in self.account_manager.accounts.values())
        net = total_profit - total_loss
        text = f"""
📊 *系统状态*

*数据状态*
• 缓存数据: {api_stats['缓存数据量']}期
• 最新期号: {api_stats['最新期号']}
• 训练状态: {'🔄 训练中' if self.model.is_training else '⏸️ 空闲'}

*账户状态*
• 总账户: {total_accounts}
• 已登录: {logged}
• 自动投注: {auto}
• 预测播报: {broadcast}

*盈利统计*
• 总盈利: {total_profit:.2f}KK
• 总亏损: {total_loss:.2f}KK
• 净盈利: {net:.2f}KK

*游戏统计*
• 投注周期: {sched_stats['game_stats']['betting_cycles']}
• 成功投注: {sched_stats['game_stats']['successful_bets']}
• 失败投注: {sched_stats['game_stats']['failed_bets']}
        """
        kb = [[InlineKeyboardButton("🔄 刷新", callback_data="refresh_status")],
              [InlineKeyboardButton("🔙 返回", callback_data="menu:main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_help_menu(self, query):
        text = """
📚 *帮助菜单*

所有操作均可通过菜单按钮完成。

• 添加账户：在“账户管理”中点击“➕ 添加账户”，输入手机号即可。
• 登录：在账户列表中选择账户，点击“🔐 登录”。
• 设置群组：进入账户详情，点击“💬 游戏群”或“📢 播报群”，从列表中选择。
• 投注设置：在“投注设置”区域选择方案、策略、金额、追号。
• 播报设置：在“播报设置”区域选择播报群和播报内容（双组/杀组）。
• 自动投注/播报：点击相应按钮即可开启/关闭（播报停止会等待最后一期开奖）。
• 查询余额/账户状态：点击相应按钮。
• 手动投注：在游戏群发送“类型 金额”即可，如“大 10000”。

如有问题，请联系管理员。
        """
        kb = [[InlineKeyboardButton("🔙 返回", callback_data="menu:main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _process_run_analysis(self, query):
        await query.edit_message_text("🔍 正在生成预测...")
        history = await self.api.get_history(50)
        if len(history) < 3:
            await query.edit_message_text("❌ 历史数据不足，至少需要3期数据")
            return
        latest = history[0]
        keno_latest = await self.api.get_latest_keno()
        if keno_latest:
            self.model.keno_similarity.current_keno_nbrs = keno_latest['nbrs']
        pred = self.model.predict(history, latest)
        acc_stats = self.model.get_accuracy_stats()
        text = f"""
🎯 *Canada28预测结果*

📊 *数据信息：*
• 最新期号: {latest.get('qihao', 'N/A')}
• 最新结果: {latest.get('sum', 'N/A')} ({latest.get('combo', 'N/A')})

🏆 *推荐预测：*
• 主推: {pred['main']}
• 候选: {pred['candidate']}
• 杀组: {pred['kill']}
• 置信度: {pred['confidence']}%

📈 *近期准确率：{acc_stats['overall']['recent']*100:.1f}%*
        """
        kb = [[InlineKeyboardButton("🔄 刷新预测", callback_data="run_analysis")],
              [InlineKeyboardButton("🔙 返回预测菜单", callback_data="menu:prediction")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _cmd_logout_inline(self, query, user, phone):
        await self.game_scheduler.stop_auto_betting(phone, user)
        await self.prediction_broadcaster.stop_broadcast(phone, user)
        client = self.account_manager.clients.get(phone)
        if client:
            try:
                if client.is_connected():
                    await client.disconnect()
                session_name = phone.replace('+', '')
                for ext in ['.session', '.session-journal', '.session.lock']:
                    file_path = Config.SESSIONS_DIR / (session_name + ext)
                    if file_path.exists():
                        file_path.unlink()
            except Exception as e:
                logger.log_error(user, f"清理客户端资源失败 {phone}", e)
            finally:
                self.account_manager.clients.pop(phone, None)
        await self.account_manager.update_account(phone,
            is_logged_in=False,
            auto_betting=False,
            prediction_broadcast=False,
            display_name='',
            chase_enabled=False,
            chase_numbers=[],
            chase_periods=0,
            chase_current=0,
            chase_amount=0,
            recommend_mode=False
        )
        self.account_manager.set_user_state(user, 'idle', {'current_account': None})
        await self._show_account_detail(query, user, phone)

# ==================== 启动 ====================
async def post_init(application):
    bot = application.bot_data.get('bot')
    if bot:
        await bot.account_manager.reset_auto_flags_on_start()
        await bot.account_manager.verify_login_status()
        await bot.account_manager.start_periodic_save()
        if hasattr(bot, 'global_scheduler'):
            await bot.global_scheduler.start()

def main():
    def handle_shutdown(signum, frame):
        print("\n🛑 接收到停止信号，正在优雅关闭...")
        if 'bot' in globals():
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(lambda: asyncio.create_task(bot.global_scheduler.stop()))
                loop.call_soon_threadsafe(lambda: asyncio.create_task(bot.account_manager.stop_periodic_save()))
                loop.call_soon_threadsafe(lambda: asyncio.create_task(bot.api.close()))
                for phone, client in bot.account_manager.clients.items():
                    if client.is_connected():
                        loop.call_soon_threadsafe(lambda: asyncio.create_task(client.disconnect()))
            except RuntimeError:
                asyncio.run(bot.global_scheduler.stop())
                asyncio.run(bot.account_manager.stop_periodic_save())
                asyncio.run(bot.api.close())
                for phone, client in bot.account_manager.clients.items():
                    if client.is_connected():
                        asyncio.run(client.disconnect())
        print("✅ 已安全关闭")
        exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("""
========================================
PC28 智能预测投注系统
========================================
启动中...
    """)
    try:
        Config.validate()
    except ValueError as e:
        print(f"❌ 配置错误: {e}")
        return
    bot = PC28Bot()
    bot.application.bot_data['bot'] = bot
    bot.application.post_init = post_init
    print("✅ Bot已启动")
    print("ℹ️ 使用 /start 开始使用")
    print("ℹ️ 管理员可使用 /train 手动触发训练，/resetmodel 重置模型权重")
    bot.application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    random.seed(time.time())
    np.random.seed(int(time.time()))
    main()
