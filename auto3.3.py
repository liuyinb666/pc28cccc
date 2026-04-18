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
from telegram.error import BadRequest, Forbidden, TimedOut


# ==================== 配置 ====================
class Config:
    # 从环境变量读取Token，支持Railway部署
    BOT_TOKEN = os.environ.get('BOT_TOKEN', '8416398436:AAGm7X4Mek9wT1BXHzkWAamZVrx59p2y0Kg')

    PC28_API_BASE = "https://www.pc28.help/"
    ADMIN_USER_IDS = [5338954122]

    DATA_DIR = Path("data")
    LOGS_DIR = DATA_DIR / "logs"
    CACHE_DIR = DATA_DIR / "cache"
    INITIAL_HISTORY_SIZE = 100
    CACHE_SIZE = 200
    DEFAULT_BASE_AMOUNT = 10000
    DEFAULT_MAX_AMOUNT = 1000000
    DEFAULT_MULTIPLIER = 2.0
    DEFAULT_STOP_LOSS = 0
    DEFAULT_STOP_WIN = 0
    DEFAULT_STOP_BALANCE = 0
    DEFAULT_RESUME_BALANCE = 0
    MIN_BET_AMOUNT = 1
    MAX_BET_AMOUNT = 10000000
    REQUEST_TIMEOUT = 15
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2
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

    LOG_RETENTION_DAYS = 7
    ACCOUNT_SAVE_INTERVAL = 30
    MAX_CONCURRENT_PREDICTIONS = 3

    # 对话状态
    ADD_ACCOUNT = 10
    CHASE_NUMBERS, CHASE_PERIODS, CHASE_AMOUNT = range(11, 14)
    SET_BASE_AMOUNT, SET_MAX_AMOUNT, SET_STOP_LOSS, SET_STOP_WIN, SET_STOP_BALANCE, SET_RESUME_BALANCE = range(20, 26)
    SET_GAME_GROUP, SET_PRED_GROUP = range(26, 28)

    MAX_ACCOUNTS_PER_USER = 5

    KENO_HISTORY_DOWNLOAD = 5000
    KJ_HISTORY_DOWNLOAD = 5000

    TRAIN_EPOCHS = 200
    MIN_TRAIN_DATA = 1000
    TRAIN_SAMPLES_PER_EPOCH = 800
    TRAIN_VALIDATION_SAMPLES = 200
    TRAIN_PATIENCE = 10
    TRAIN_LR = 0.001
    TRAIN_LR_DECAY = 0.5
    TRAIN_COOLDOWN = 5

    PREDICTION_HISTORY_SIZE = 20

    RISK_PROFILES = {
        '保守': 0.005,
        '稳定': 0.01,
        '激进': 0.02,
    }

    @classmethod
    def init_dirs(cls):
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.LOGS_DIR.mkdir(exist_ok=True)
        cls.CACHE_DIR.mkdir(exist_ok=True)

    @classmethod
    def validate(cls):
        errors = []
        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN未配置")
        if not cls.PC28_API_BASE.startswith(('http://', 'https://')):
            errors.append("PC28_API_BASE必须是有效的URL")
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
    def log_account(self, user_id, name, action): self.logger.info(f"[账户] 用户:{user_id} 账户:{name} {action}")
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


logger = BotLogger()


# ==================== 基础数据 ====================
COMBOS = ["小单", "小双", "大单", "大双"]
BASE_PROB = {"小单": 27.11, "小双": 23.83, "大单": 22.32, "大双": 26.74}
TRANSITION_MATRIX = {
    "小单": {"小单": 26.3, "小双": 23.9, "大单": 22.9, "大双": 26.9},
    "小双": {"小单": 27.2, "小双": 22.7, "大单": 22.9, "大双": 27.3},
    "大单": {"小单": 28.2, "小双": 23.9, "大单": 21.5, "大双": 26.5},
    "大双": {"小单": 27.0, "小双": 24.7, "大单": 21.9, "大双": 26.4}
}


# ==================== 预测算法 ====================
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
        if len(set([x['combo'] for x in h])) == 1:
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
        return TRANSITION_MATRIX.get(latest['combo'], TRANSITION_MATRIX["大单"])

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
        return {c: (1 - freq[c]/total) * 100 for c in COMBOS}

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
                return {c: outcomes.get(c, 0) / total * 100 for c in COMBOS}
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
        return {'hourly_stats': {}, 'daily_stats': {}, 'position_stats': {}, 'accuracy_history': []}

    def save_memory(self):
        try:
            with open(self.memory_file, 'w', encoding='utf-8') as f:
                json.dump(self.memory, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.log_error(0, "保存长期记忆失败", e)

    def learn(self, history):
        for h in history:
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


# ==================== Keno相似性分析 ====================
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
                    weighted_counts[next_kj['combo']] += weight
                    total_weight += weight
        if total_weight == 0:
            return {c: 25 for c in COMBOS}
        return {c: weighted_counts[c] / total_weight * 100 for c in COMBOS}

    def save_model(self):
        try:
            model_data = {'keno_history': list(self.keno_history), 'kj_history': list(self.kj_history)}
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
        self.combo_to_index = {combo: i for i, combo in enumerate(COMBOS)}
        self.index_to_combo = {i: combo for i, combo in enumerate(COMBOS)}
        self.model_file = Config.CACHE_DIR / "rl_model.json"
        self.training_history = deque(maxlen=1000)

    def _state_to_key(self, recent_combos):
        if len(recent_combos) < 3:
            return None
        key = 0
        for combo in recent_combos[-3:]:
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
            'state': state_key, 'action': action, 'reward': reward,
            'next_state': next_state_key, 'timestamp': datetime.now().isoformat()
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


# ==================== 模型管理器 ====================
class ModelManager:
    def __init__(self):
        self.weights = {
            "概率分布": 2.5, "趋势分析": 2.5, "和值分析": 2.5, "冷热分析": 2.5,
            "转移概率": 2.5, "连续模式": 2.5, "均衡回归": 2.5, "综合推荐": 2.5,
            "Keno相似性": 2.5, "强化学习": 2.5, "模式识别": 2.5,
            "小时规律": 2.5, "位置规律": 2.5, "趋势强化": 2.5, "理论概率": 3.0,
        }
        self.exploration_rate = Config.EXPLORATION_RATE
        self.prediction_history = []
        self.algos = [
            ("概率分布", Algorithms.prob_dist), ("趋势分析", Algorithms.trend),
            ("和值分析", Algorithms.sum_analysis), ("冷热分析", Algorithms.cold_hot),
            ("转移概率", Algorithms.transition), ("连续模式", Algorithms.continuous),
            ("均衡回归", Algorithms.equilibrium), ("综合推荐", Algorithms.comprehensive),
            ("Keno相似性", self._keno_scores), ("强化学习", self._rl_scores),
            ("模式识别", self._pattern_scores), ("小时规律", self._hour_scores),
            ("位置规律", self._position_scores), ("趋势强化", self._trend_scores),
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
                data = {'weights': self.weights, 'exploration_rate': self.exploration_rate,
                        'last_save': datetime.now().isoformat()}
                async with aiofiles.open(Config.MODEL_SAVE_FILE, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            except Exception as e:
                logger.log_error(0, "保存模型权重失败", e)

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
            scores = self.long_term_memory.get_position_scores(qihao[-2:])
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

        return {
            "main": main, "candidate": candidate, "kill": kill,
            "confidence": confidence, "scores": final_scores,
            "algo_details": [(name, scores) for name, scores in algo_scores_list],
        }

    async def learn(self, prediction, actual, qihao, sum_val):
        main = prediction['main']
        candidate = prediction['candidate']
        kill = prediction['kill']
        is_correct_double = (actual == main or actual == candidate)
        is_correct_kill = (actual != kill)

        record = {
            "time": datetime.now().isoformat(), "qihao": qihao,
            "main": main, "candidate": candidate, "kill": kill,
            "actual": actual, "sum": sum_val,
            "correct_double": is_correct_double, "correct_kill": is_correct_kill
        }
        self.prediction_history.append(record)
        self.recent_accuracy.append(1 if is_correct_double else 0)

        recent_correct = sum(self.recent_accuracy)
        recent_total = len(self.recent_accuracy)
        current_accuracy = recent_correct / recent_total if recent_total > 0 else 0.5
        learning_rate = 0.05 * (1 - current_accuracy)

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

        self.exploration_rate = max(Config.EXPLORATION_MIN, self.exploration_rate * Config.EXPLORATION_DECAY)
        asyncio.create_task(self.save())
        if random.random() < 0.01:
            self.rl_model.save()

    def get_accuracy_stats(self):
        return {
            'overall': {
                'recent': sum(self.recent_accuracy) / len(self.recent_accuracy) if self.recent_accuracy else 0,
                'total': sum(1 for r in self.prediction_history if r.get('correct_double', False)) / len(self.prediction_history) if self.prediction_history else 0
            }
        }

    async def offline_train(self, kj_data, keno_data, max_epochs=Config.TRAIN_EPOCHS):
        logger.log_system("训练功能暂未实现")
        pass

    async def _check_new_result(self):
        return False


# ==================== API模块 ====================
class PC28API:
    def __init__(self):
        self.base_url = Config.PC28_API_BASE
        self.session = None
        self.cache_file = Config.CACHE_DIR / "history_cache.pkl"
        self.keno_cache_file = Config.CACHE_DIR / "keno_cache.pkl"
        self.history_cache = deque(maxlen=Config.CACHE_SIZE)
        self.keno_cache = deque(maxlen=5000)
        self.load_cache()
        logger.log_system("异步API模块初始化完成")

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT))

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
            try:
                url = f"{self.base_url}/{endpoint}.json"
                if params:
                    query_string = "&".join(f"{k}={v}" for k, v in params.items())
                    url = f"{url}?{query_string}"
                async with self.session.get(url) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    if data.get('message') != 'success':
                        if retry < Config.MAX_RETRIES - 1:
                            await asyncio.sleep(Config.RETRY_BACKOFF ** retry)
                            continue
                        return None
                    return data.get('data', [])
            except Exception as e:
                if retry < Config.MAX_RETRIES - 1:
                    await asyncio.sleep(Config.RETRY_BACKOFF ** retry)
                else:
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
                return [{k.strip(): v.strip() for k, v in row.items()} for row in reader]
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
                'qihao': qihao, 'opentime': f"{date_str} {time_str}", 'opennum': str(total) if total else '',
                'sum': total, 'size': size, 'parity': parity, 'combo': combo,
                'parsed_time': self._parse_time(date_str, time_str),
                'fetch_time': datetime.now().isoformat(),
                'hash': hashlib.md5(f"{qihao}_{total}".encode()).hexdigest()[:8]
            }
        except Exception:
            return None

    def _parse_keno_csv_row(self, row: Dict) -> Optional[Dict]:
        try:
            qihao = row.get('期号', '').strip()
            date = row.get('日期', '').strip()
            time_str = row.get('时间', '').strip()
            nbrs_str = row.get('开奖号码', '').strip()
            nbrs_str = nbrs_str.strip('"')
            nbrs = [int(x.strip()) for x in nbrs_str.split(',')]
            if len(nbrs) != 20:
                return None
            return {
                'qihao': qihao, 'nbrs': nbrs, 'date': date,
                'time': time_str, 'bonus': row.get('奖金', ''),
                'parsed_time': self._parse_time(date, time_str),
            }
        except Exception:
            return None

    async def fetch_kj(self, nbr=1):
        data = await self._make_api_call('kj', {'nbr': nbr})
        if not data:
            return []
        processed = []
        for item in data:
            try:
                qihao = str(item.get('nbr', '')).strip()
                number = item.get('number') or item.get('num')
                if not number:
                    continue
                if isinstance(number, str) and '+' in number:
                    parts = number.split('+')
                    if len(parts) == 3:
                        total = sum(int(p) for p in parts)
                    else:
                        continue
                else:
                    total = int(number)
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
                processed.append({
                    'qihao': qihao, 'opentime': f"{date_str} {time_str}", 'opennum': str(total),
                    'sum': total, 'size': size, 'parity': parity, 'combo': combo,
                    'parsed_time': self._parse_time(date_str, time_str),
                    'fetch_time': datetime.now().isoformat(),
                    'hash': hashlib.md5(f"{qihao}_{total}".encode()).hexdigest()[:8]
                })
            except Exception:
                continue
        processed.sort(key=lambda x: x.get('parsed_time', datetime.now()), reverse=True)
        return processed

    async def fetch_keno(self, nbr=1):
        data = await self._make_api_call('keno', {'nbr': nbr})
        if not data:
            return []
        processed = []
        for item in data:
            try:
                qihao = str(item.get('nbr', ''))
                nbrs_str = item.get('nbrs', '')
                if not nbrs_str:
                    continue
                nbrs = []
                for x in nbrs_str.split(','):
                    x = x.strip()
                    if x and x.isdigit():
                        nbrs.append(int(x))
                if len(nbrs) != 20:
                    continue
                date_str = item.get('date', '')
                time_str = item.get('time', '')
                processed.append({
                    'qihao': qihao, 'nbrs': nbrs, 'date': date_str,
                    'time': time_str, 'bonus': item.get('bonus', ''),
                    'parsed_time': self._parse_time(date_str, time_str),
                })
            except Exception:
                continue
        processed.sort(key=lambda x: x.get('parsed_time', datetime.now()), reverse=True)
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
        logger.log_system("正在初始化历史数据...")
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
            return len(self.history_cache) >= 30

        logger.log_system("CSV下载失败，回退到API...")
        for attempt in range(max_retries):
            kj_data = await self.fetch_kj(nbr=count)
            if kj_data:
                self.history_cache.clear()
                for item in kj_data:
                    if not any(x.get('qihao') == item['qihao'] for x in self.history_cache):
                        self.history_cache.append(item)
                self.save_cache()
                logger.log_system(f"历史数据初始化完成: {len(self.history_cache)}条")
                return True
            await asyncio.sleep(2)
        return False

    async def get_latest_result(self):
        latest_api = await self.fetch_kj(nbr=1)
        if not latest_api:
            return None
        latest = latest_api[0]
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

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def get_statistics(self):
        return {
            '缓存数据量': len(self.history_cache),
            'Keno缓存': len(self.keno_cache),
            '最新期号': self.history_cache[0].get('qihao') if self.history_cache else '无'
        }


# ==================== 账户模型（简化版） ====================
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
    name: str
    owner_user_id: int
    created_time: str = field(default_factory=lambda: datetime.now().isoformat())
    auto_betting: bool = False
    prediction_broadcast: bool = False
    game_group_id: int = 0
    game_group_name: str = ""
    prediction_group_id: int = 0
    prediction_group_name: str = ""
    betting_strategy: str = "保守"
    betting_scheme: str = "组合1"
    bet_params: BetParams = field(default_factory=BetParams)
    consecutive_losses: int = 0
    consecutive_wins: int = 0
    total_bets: int = 0
    total_wins: int = 0
    last_bet_period: Optional[str] = None
    last_bet_types: List[str] = field(default_factory=list)
    last_bet_amount: int = 0
    last_prediction: Dict = field(default_factory=dict)
    martingale_reset: bool = True
    fibonacci_reset: bool = True
    chase_enabled: bool = False
    chase_numbers: List[int] = field(default_factory=list)
    chase_periods: int = 0
    chase_current: int = 0
    chase_amount: int = 0
    chase_stop_reason: Optional[str] = None
    recommend_mode: bool = False
    risk_profile: str = "稳定"
    prediction_content: str = "double"
    broadcast_stop_requested: bool = False
    input_mode: Optional[str] = None
    last_message_id: Optional[int] = None

    def get_display_name(self) -> str:
        return self.name

    def get_risk_factor(self) -> float:
        return Config.RISK_PROFILES.get(self.risk_profile, 0.01)


# ==================== 账户管理器 ====================
class AccountManager:
    def __init__(self):
        self.accounts_file = Config.DATA_DIR / "accounts.json"
        self.user_states_file = Config.DATA_DIR / "user_states.json"
        self.accounts: Dict[str, Account] = {}
        self.user_states: Dict[int, Dict] = {}
        self.update_lock = asyncio.Lock()
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
                for name, acc_dict in data.items():
                    bet_params_dict = acc_dict.get('bet_params', {})
                    bet_params = BetParams(**bet_params_dict)
                    acc_dict['bet_params'] = bet_params
                    defaults = {
                        'chase_enabled': False, 'chase_numbers': [], 'chase_periods': 0,
                        'chase_current': 0, 'chase_amount': 0, 'chase_stop_reason': None,
                        'recommend_mode': False, 'risk_profile': "稳定",
                        'prediction_content': "double", 'broadcast_stop_requested': False,
                        'input_mode': None, 'last_message_id': None,
                        'martingale_reset': True, 'fibonacci_reset': True
                    }
                    for k, v in defaults.items():
                        if k not in acc_dict:
                            acc_dict[k] = v
                    self.accounts[name] = Account(**acc_dict)
            except Exception as e:
                logger.log_error(0, "加载账户数据失败", e)

    async def save_accounts(self):
        data = {}
        for name, acc in self.accounts.items():
            data[name] = asdict(acc)
        try:
            async with aiofiles.open(self.accounts_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception as e:
            logger.log_error(0, "保存账户数据失败", e)

    async def _periodic_save(self):
        while True:
            await asyncio.sleep(Config.ACCOUNT_SAVE_INTERVAL)
            async with self.update_lock:
                dirty = self._dirty.copy() if self._dirty else None
                self._dirty.clear()
            if dirty:
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

    async def add_account(self, user_id, name) -> Tuple[bool, str]:
        async with self.update_lock:
            if user_id not in Config.ADMIN_USER_IDS:
                user_accounts = [acc for acc in self.accounts.values() if acc.owner_user_id == user_id]
                if len(user_accounts) >= Config.MAX_ACCOUNTS_PER_USER:
                    return False, f"每个用户最多只能添加 {Config.MAX_ACCOUNTS_PER_USER} 个账户"
            if name in self.accounts:
                return False, "账户名称已存在"
            if not re.match(r'^[a-zA-Z0-9\u4e00-\u9fa5_]{1,20}$', name):
                return False, "账户名称格式不正确（1-20个字符）"
            self.accounts[name] = Account(name=name, owner_user_id=user_id)
            self._dirty.add(name)
            logger.log_account(user_id, name, "添加账户")
            return True, f"账户 {name} 添加成功"

    def get_account(self, name) -> Optional[Account]:
        return self.accounts.get(name)

    async def update_account(self, name, **kwargs):
        async with self.update_lock:
            if name in self.accounts:
                acc = self.accounts[name]
                for k, v in kwargs.items():
                    if k == 'bet_params' and isinstance(v, dict):
                        for pk, pv in v.items():
                            setattr(acc.bet_params, pk, pv)
                    else:
                        setattr(acc, k, v)
                self._dirty.add(name)
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

    async def start_periodic_save(self):
        self._save_task = asyncio.create_task(self._periodic_save())

    async def stop_periodic_save(self):
        if self._save_task:
            self._save_task.cancel()
            try:
                await self._save_task
            except asyncio.CancelledError:
                pass


# ==================== 消息发送器（Bot直接发送） ====================
class MessageSender:
    def __init__(self, application: Application):
        self.application = application
        self._send_locks: Dict[int, asyncio.Lock] = {}

    async def send_message(self, chat_id: int, text: str) -> Optional[int]:
        if chat_id not in self._send_locks:
            self._send_locks[chat_id] = asyncio.Lock()
        async with self._send_locks[chat_id]:
            try:
                msg = await self.application.bot.send_message(chat_id=chat_id, text=text)
                return msg.message_id
            except Forbidden:
                logger.log_error(0, f"发送消息失败: Bot不在群组 {chat_id} 中", None)
                return None
            except TimedOut:
                logger.log_error(0, f"发送消息超时: {chat_id}", None)
                return None
            except Exception as e:
                logger.log_error(0, f"发送消息失败: {chat_id}", e)
                return None

    async def send_bet_message(self, chat_id: int, bet_items: List[str]) -> bool:
        message = " ".join(bet_items)
        logger.log_betting(0, "发送投注", f"群组:{chat_id} 消息:{message}")
        msg_id = await self.send_message(chat_id, message)
        return msg_id is not None


# ==================== 投注执行器 ====================
class BetExecutor:
    def __init__(self, account_manager: AccountManager, message_sender: MessageSender):
        self.account_manager = account_manager
        self.message_sender = message_sender
        self.game_stats = {'successful_bets': 0, 'failed_bets': 0}

    def _calculate_bet_amount(self, acc: Account) -> Tuple[int, Dict]:
        base = acc.bet_params.base_amount
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
        if scheme == '杀主':
            return [c for c in COMBOS if c != pred['kill']]
        return [rec[0]] if rec else ['小双']

    async def execute_bet(self, name: str, prediction: Dict, latest: Dict) -> bool:
        acc = self.account_manager.get_account(name)
        if not acc or not acc.auto_betting:
            return False

        current_qihao = latest.get('qihao')
        if acc.last_bet_period == current_qihao:
            return False

        now = datetime.now()
        next_open = latest['parsed_time'] + timedelta(seconds=Config.GAME_CYCLE_SECONDS)
        close_time = next_open - timedelta(seconds=Config.CLOSE_BEFORE_SECONDS)
        if now >= close_time:
            logger.log_betting(0, "已封盘，跳过投注", f"账户:{name}")
            return False

        bet_types = self._get_bet_types(prediction, acc.betting_scheme)
        bet_amount, updates = self._calculate_bet_amount(acc)
        if updates:
            await self.account_manager.update_account(name, **updates)

        if not acc.game_group_id:
            logger.log_betting(0, "未设置游戏群", f"账户:{name}")
            return False

        bet_items = [f"{t} {bet_amount}" for t in bet_types]
        success = await self.message_sender.send_bet_message(acc.game_group_id, bet_items)

        if success:
            self.game_stats['successful_bets'] += 1
            await self.account_manager.update_account(
                name,
                last_bet_period=current_qihao,
                last_bet_types=bet_types,
                last_bet_amount=bet_amount,
                total_bets=acc.total_bets + 1,
                last_prediction={
                    'main': prediction['main'], 'candidate': prediction['candidate'],
                    'confidence': prediction['confidence'], 'kill': prediction['kill']
                }
            )
            logger.log_betting(0, "投注成功", f"账户:{name} 金额:{bet_amount} 类型:{bet_types}")
        else:
            self.game_stats['failed_bets'] += 1
            logger.log_betting(0, "投注失败", f"账户:{name}")

        return success

    async def execute_chase(self, name: str, latest: dict) -> bool:
        acc = self.account_manager.get_account(name)
        if not acc or not acc.chase_enabled:
            return False

        if acc.chase_current >= acc.chase_periods:
            await self.account_manager.update_account(name, chase_enabled=False, chase_stop_reason="期满")
            return False

        current_qihao = latest.get('qihao')
        if acc.last_bet_period == current_qihao:
            return False

        bet_amount = acc.chase_amount if acc.chase_amount > 0 else acc.bet_params.base_amount
        bet_amount = min(bet_amount, acc.bet_params.max_amount)
        bet_amount = max(bet_amount, Config.MIN_BET_AMOUNT)

        if not acc.game_group_id:
            return False

        bet_items = [f"{num} {bet_amount}" for num in acc.chase_numbers]
        success = await self.message_sender.send_bet_message(acc.game_group_id, bet_items)

        if success:
            await self.account_manager.update_account(
                name,
                chase_current=acc.chase_current + 1,
                last_bet_period=current_qihao,
                total_bets=acc.total_bets + 1
            )
            logger.log_betting(0, "追号成功", f"账户:{name} 进度:{acc.chase_current+1}/{acc.chase_periods}")
            return True
        return False

    def check_bet_result(self, name: str, expected_qihao: str, latest_result: dict):
        acc = self.account_manager.get_account(name)
        if not acc:
            return

        actual_combo = latest_result.get('combo')
        if not actual_combo:
            return

        last_pred = acc.last_prediction
        scheme = acc.betting_scheme

        def is_match(bet_type: str, actual: str) -> bool:
            if bet_type == actual:
                return True
            if bet_type in ["大", "小"] and actual.startswith(bet_type):
                return True
            if bet_type in ["单", "双"] and actual[1] == bet_type:
                return True
            return False

        if scheme == '杀主':
            is_win = actual_combo != last_pred.get('kill')
        else:
            bet_types = self._get_bet_types(last_pred, scheme)
            is_win = any(is_match(t, actual_combo) for t in bet_types)

        if is_win:
            asyncio.create_task(self.account_manager.update_account(
                name, consecutive_wins=acc.consecutive_wins + 1, consecutive_losses=0,
                martingale_reset=True, fibonacci_reset=True, total_wins=acc.total_wins + 1
            ))
        else:
            asyncio.create_task(self.account_manager.update_account(
                name, consecutive_losses=acc.consecutive_losses + 1, consecutive_wins=0
            ))

    def get_stats(self):
        auto = sum(1 for a in self.account_manager.accounts.values() if a.auto_betting)
        broadcast = sum(1 for a in self.account_manager.accounts.values() if a.prediction_broadcast)
        return {'auto_betting_accounts': auto, 'broadcast_accounts': broadcast, 'game_stats': self.game_stats}


# ==================== 预测播报器 ====================
class PredictionBroadcaster:
    def __init__(self, account_manager: AccountManager, model_manager, api_client, message_sender: MessageSender):
        self.account_manager = account_manager
        self.model = model_manager
        self.api = api_client
        self.message_sender = message_sender
        self.broadcast_tasks: Dict[str, asyncio.Task] = {}
        self.global_predictions = {
            'predictions': [], 'last_open_qihao': None, 'next_qihao': None,
            'last_update': None, 'cached_double_message': None, 'cached_kill_message': None
        }
        self.last_sent_qihao: Dict[str, str] = {}
        self.stop_target_qihao: Dict[str, str] = {}

    async def start_broadcast(self, name: str, user_id: int) -> Tuple[bool, str]:
        acc = self.account_manager.get_account(name)
        if not acc:
            return False, "账户不存在"
        if not acc.prediction_group_id:
            return False, "请先设置播报群"
        if name in self.broadcast_tasks and not self.broadcast_tasks[name].done():
            return True, "播报器已在运行"
        if name in self.broadcast_tasks:
            self.broadcast_tasks[name].cancel()
        self.last_sent_qihao[name] = self.global_predictions.get('next_qihao')
        task = asyncio.create_task(self._broadcast_loop(name, acc.prediction_group_id))
        self.broadcast_tasks[name] = task
        await self.account_manager.update_account(name, prediction_broadcast=True)
        logger.log_prediction(user_id, "播报器启动", f"账户:{name}")
        return True, "预测播报器启动成功"

    async def stop_broadcast(self, name: str, user_id: int) -> Tuple[bool, str]:
        acc = self.account_manager.get_account(name)
        if not acc or not acc.prediction_broadcast:
            return True, "播报器已停止"
        target = self.global_predictions.get('next_qihao')
        await self.account_manager.update_account(name, broadcast_stop_requested=True)
        self.stop_target_qihao[name] = target
        logger.log_prediction(user_id, "播报器平滑停止请求", f"账户:{name}")
        return True, "将在最后一期开奖后停止播报"

    async def _broadcast_loop(self, name: str, group_id: int):
        target_qihao = None
        while True:
            try:
                acc = self.account_manager.get_account(name)
                if not acc:
                    break

                if acc.broadcast_stop_requested:
                    if target_qihao is None:
                        target_qihao = self.stop_target_qihao.get(name, self.global_predictions.get('next_qihao'))
                    if self.last_sent_qihao.get(name) != target_qihao:
                        await self._send_prediction(group_id, name)
                    if self.global_predictions.get('last_open_qihao') == target_qihao:
                        await self.account_manager.update_account(name, prediction_broadcast=False, broadcast_stop_requested=False)
                        break
                elif acc.prediction_broadcast:
                    await self._send_prediction(group_id, name)

                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.log_error(0, f"播报器异常 {name}", e)
                await asyncio.sleep(10)

    async def _send_prediction(self, group_id: int, name: str) -> Optional[int]:
        acc = self.account_manager.get_account(name)
        if not acc:
            return None

        current_qihao = self.global_predictions.get('next_qihao')
        if self.last_sent_qihao.get(name) == current_qihao:
            return None

        if acc.prediction_content == "double":
            message = self.global_predictions.get('cached_double_message')
        else:
            message = self.global_predictions.get('cached_kill_message')

        if not message:
            self._update_cached_messages()
            message = self.global_predictions['cached_double_message'] if acc.prediction_content == "double" else self.global_predictions['cached_kill_message']

        msg_id = await self.message_sender.send_message(group_id, message)
        if msg_id:
            self.last_sent_qihao[name] = current_qihao
            await self.account_manager.update_account(name, last_message_id=msg_id)
        return msg_id

    def _update_cached_messages(self):
        lines = ["🤖 强化学习中", "-" * 30, "期号    主推候选  状态  和值"]
        for p in self.global_predictions['predictions'][-Config.PREDICTION_HISTORY_SIZE:]:
            q = p['qihao'][-4:] if len(p['qihao']) >= 4 else p['qihao']
            combo_str = p['main'] + p['candidate']
            mark = "✅" if p.get('correct_double') is True else "❌" if p.get('correct_double') is False else "⏳"
            s = str(p['sum']) if p['sum'] is not None else "--"
            lines.append(f"{q:4s}   {combo_str:4s}   {mark:2s}   {s:>2s}")
        self.global_predictions['cached_double_message'] = "AI双组预测\n```\n" + "\n".join(lines) + "\n```"

        kill_lines = ["🤖 Keno暗线匹配灰盒杀", "-" * 30, "期号    杀组    状态  和值"]
        for p in self.global_predictions['predictions'][-Config.PREDICTION_HISTORY_SIZE:]:
            q = p['qihao'][-4:] if len(p['qihao']) >= 4 else p['qihao']
            kill = p.get('kill', '--')
            mark = "✅" if p.get('correct_kill') is True else "❌" if p.get('correct_kill') is False else "⏳"
            s = str(p['sum']) if p['sum'] is not None else "--"
            kill_lines.append(f"{q:4s}   {kill:4s}   {mark:2s}   {s:>2s}")
        self.global_predictions['cached_kill_message'] = "AI杀组预测\n```\n" + "\n".join(kill_lines) + "\n```"

    async def update_global_predictions(self, prediction, next_qihao, latest):
        current_qihao = latest.get('qihao')
        current_combo = latest.get('combo')
        current_sum = latest.get('sum')

        # 检查上一期预测是否正确
        for p in self.global_predictions['predictions']:
            if p.get('qihao') == current_qihao:
                p['actual'] = current_combo
                p['sum'] = current_sum
                p['correct_double'] = (p['main'] == current_combo or p['candidate'] == current_combo)
                p['correct_kill'] = (p['kill'] != current_combo)
                await self.model.learn(p, current_combo, current_qihao, current_sum)
                break

        # 添加新预测
        self.global_predictions['predictions'].append({
            'qihao': next_qihao, 'main': prediction['main'], 'candidate': prediction['candidate'],
            'kill': prediction['kill'], 'confidence': prediction['confidence'],
            'time': datetime.now().isoformat(), 'actual': None, 'sum': None,
            'correct_double': None, 'correct_kill': None
        })
        if len(self.global_predictions['predictions']) > Config.PREDICTION_HISTORY_SIZE:
            self.global_predictions['predictions'] = self.global_predictions['predictions'][-Config.PREDICTION_HISTORY_SIZE:]

        self.global_predictions['last_open_qihao'] = current_qihao
        self.global_predictions['next_qihao'] = next_qihao
        self.global_predictions['last_update'] = datetime.now().isoformat()
        self._update_cached_messages()


# ==================== 全局调度器 ====================
class GlobalScheduler:
    def __init__(self, account_manager, model_manager, api_client,
                 prediction_broadcaster, bet_executor):
        self.account_manager = account_manager
        self.model = model_manager
        self.api = api_client
        self.prediction_broadcaster = prediction_broadcaster
        self.bet_executor = bet_executor
        self.task = None
        self.running = False
        self.last_qihao = None
        self.check_interval = Config.SCHEDULER_CHECK_INTERVAL
        self.tasks = set()

    async def start(self):
        if self.running:
            return
        self.running = True
        self.task = asyncio.create_task(self._run())
        self.tasks.add(self.task)
        logger.log_system("全局调度器已启动")

    async def stop(self):
        self.running = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.log_system("全局调度器已停止")

    async def _run(self):
        init_success = False
        for attempt in range(5):
            if await self.api.initialize_history():
                init_success = True
                break
            await asyncio.sleep(5)

        while self.running:
            try:
                latest = await self.api.get_latest_result()
                if latest:
                    qihao = latest.get('qihao')
                    if qihao != self.last_qihao:
                        logger.log_game(f"检测到新期号: {qihao}")
                        await self._on_new_period(qihao, latest)
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.log_error(0, "全局调度器异常", e)
                await asyncio.sleep(10)

    async def _on_new_period(self, qihao, latest):
        try:
            # 检查投注结果
            for name, acc in self.account_manager.accounts.items():
                if acc.last_bet_period and acc.last_bet_period != qihao:
                    self.bet_executor.check_bet_result(name, acc.last_bet_period, latest)

            # 更新Keno数据
            keno = await self.api.get_latest_keno()
            if keno:
                self.model.keno_similarity.add_keno_data(keno, latest)
                self.model.current_keno_nbrs = keno['nbrs']

            history = await self.api.get_history(50)
            if len(history) < 3:
                return

            prediction = self.model.predict(history, latest)
            next_qihao = increment_qihao(qihao)

            await self.prediction_broadcaster.update_global_predictions(prediction, next_qihao, latest)

            # 执行自动投注
            for name, acc in self.account_manager.accounts.items():
                if acc.auto_betting and acc.game_group_id:
                    await self.bet_executor.execute_chase(name, latest)
                    await self.bet_executor.execute_bet(name, prediction, latest)

            self.last_qihao = qihao
        except Exception as e:
            logger.log_error(0, f"处理新期号失败 {qihao}", e)


# ==================== 主Bot类 ====================
class PC28Bot:
    def __init__(self):
        self.api = PC28API()
        self.account_manager = AccountManager()
        self.model = ModelManager()
        self.application = Application.builder().token(Config.BOT_TOKEN).build()
        self.message_sender = MessageSender(self.application)
        self.bet_executor = BetExecutor(self.account_manager, self.message_sender)
        self.prediction_broadcaster = PredictionBroadcaster(
            self.account_manager, self.model, self.api, self.message_sender)
        self.global_scheduler = GlobalScheduler(
            self.account_manager, self.model, self.api,
            self.prediction_broadcaster, self.bet_executor)
        self._register_handlers()
        logger.log_system("PC28 Bot 初始化完成")

    def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("cancel", self.cmd_cancel))

        # 添加账户
        add_account_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_account_start, pattern=r'^add_account$')],
            states={Config.ADD_ACCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_account_input)]},
            fallbacks=[CommandHandler('cancel', self.cmd_cancel)],
        )
        self.application.add_handler(add_account_conv)

        # 设置游戏群
        set_group_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.set_group_start, pattern=r'^action:setgroup:([^:]+)$')],
            states={Config.SET_GAME_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.set_group_input)]},
            fallbacks=[CommandHandler('cancel', self.cmd_cancel)],
        )
        self.application.add_handler(set_group_conv)

        # 设置播报群
        set_pred_group_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.set_pred_group_start, pattern=r'^action:setpredgroup:([^:]+)$')],
            states={Config.SET_PRED_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.set_pred_group_input)]},
            fallbacks=[CommandHandler('cancel', self.cmd_cancel)],
        )
        self.application.add_handler(set_pred_group_conv)

        # 金额设置
        for param, state in [('base_amount', Config.SET_BASE_AMOUNT), ('max_amount', Config.SET_MAX_AMOUNT),
                              ('stop_loss', Config.SET_STOP_LOSS), ('stop_win', Config.SET_STOP_WIN),
                              ('stop_balance', Config.SET_STOP_BALANCE), ('resume_balance', Config.SET_RESUME_BALANCE)]:
            conv = ConversationHandler(
                entry_points=[CallbackQueryHandler(self.amount_set_start, pattern=f'^amount_set:{param}:([^:]+)$')],
                states={state: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.amount_set_input)]},
                fallbacks=[CommandHandler('cancel', self.cmd_cancel)],
            )
            self.application.add_handler(conv)

        # 追号
        chase_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.chase_start, pattern=r'^action:setchase:([^:]+)$')],
            states={Config.CHASE_NUMBERS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.chase_input_numbers)],
                    Config.CHASE_PERIODS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.chase_input_periods)],
                    Config.CHASE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.chase_input_amount)]},
            fallbacks=[CommandHandler('cancel', self.cmd_cancel)],
        )
        self.application.add_handler(chase_conv)

        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text_message))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        self.application.add_error_handler(self.error_handler)

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
            "✨ Bot会直接向游戏群发送投注消息，无需登录Telegram账号！\n\n"
            "请选择操作：",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown'
        )

    async def add_account_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        await query.edit_message_text(
            "📱 *添加账户*\n\n请输入账户名称（1-20个字符，支持中文、字母、数字）：\n\n点击 /cancel 取消",
            parse_mode='Markdown'
        )
        return Config.ADD_ACCOUNT

    async def add_account_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        name = update.message.text.strip()
        ok, msg = await self.account_manager.add_account(user_id, name)
        if ok:
            await update.message.reply_text(f"✅ {msg}")
            await self._show_account_detail(update.message, user_id, name)
        else:
            await update.message.reply_text(f"❌ {msg}")
            await self._show_main_menu(update.message)
        return ConversationHandler.END

    async def set_group_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        name = query.data.split(':')[1]
        context.user_data['set_group_name'] = name
        await query.edit_message_text(
            "💬 *设置游戏群*\n\n"
            "请将Bot添加到您的游戏群中，然后在此输入群ID。\n\n"
            "如何获取群ID？\n"
            "1. 将 @userinfobot 添加到群组\n"
            "2. 发送 /start\n"
            "3. 机器人会返回群ID（负数）\n\n"
            "请输入群ID：\n\n点击 /cancel 取消",
            parse_mode='Markdown'
        )
        return Config.SET_GAME_GROUP

    async def set_group_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        name = context.user_data.get('set_group_name')
        if not name:
            await update.message.reply_text("❌ 会话已过期")
            return ConversationHandler.END
        try:
            group_id = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("❌ 请输入有效的数字ID")
            return Config.SET_GAME_GROUP
        await self.account_manager.update_account(name, game_group_id=group_id, game_group_name=str(group_id))
        await update.message.reply_text(f"✅ 游戏群ID已设置为 {group_id}")
        await self._show_account_detail(update.message, update.effective_user.id, name)
        context.user_data.pop('set_group_name', None)
        return ConversationHandler.END

    async def set_pred_group_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        name = query.data.split(':')[1]
        context.user_data['set_pred_name'] = name
        await query.edit_message_text(
            "📢 *设置播报群*\n\n"
            "请将Bot添加到您的播报群中，然后在此输入群ID。\n\n"
            "请输入群ID：\n\n点击 /cancel 取消",
            parse_mode='Markdown'
        )
        return Config.SET_PRED_GROUP

    async def set_pred_group_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        name = context.user_data.get('set_pred_name')
        if not name:
            await update.message.reply_text("❌ 会话已过期")
            return ConversationHandler.END
        try:
            group_id = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("❌ 请输入有效的数字ID")
            return Config.SET_PRED_GROUP
        await self.account_manager.update_account(name, prediction_group_id=group_id, prediction_group_name=str(group_id))
        await update.message.reply_text(f"✅ 播报群ID已设置为 {group_id}")
        await self._show_account_detail(update.message, update.effective_user.id, name)
        context.user_data.pop('set_pred_name', None)
        return ConversationHandler.END

    async def amount_set_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        param_name = query.data.split(':')[1]
        name = query.data.split(':')[2]
        context.user_data['amount_param'] = param_name
        context.user_data['amount_name'] = name
        param_display = {'base_amount': '基础金额', 'max_amount': '最大金额', 'stop_loss': '止损金额',
                         'stop_win': '止盈金额', 'stop_balance': '停止余额', 'resume_balance': '恢复余额'}.get(param_name, param_name)
        await query.edit_message_text(f"🔢 请输入新的 {param_display}（整数KK）：\n\n点击 /cancel 取消")
        state_map = {'base_amount': Config.SET_BASE_AMOUNT, 'max_amount': Config.SET_MAX_AMOUNT,
                     'stop_loss': Config.SET_STOP_LOSS, 'stop_win': Config.SET_STOP_WIN,
                     'stop_balance': Config.SET_STOP_BALANCE, 'resume_balance': Config.SET_RESUME_BALANCE}
        return state_map.get(param_name, Config.SET_BASE_AMOUNT)

    async def amount_set_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        name = context.user_data.get('amount_name')
        param_name = context.user_data.get('amount_param')
        if not name or not param_name:
            await update.message.reply_text("❌ 会话已过期")
            return ConversationHandler.END
        try:
            amount = int(update.message.text.strip())
            if amount < 0:
                await update.message.reply_text("❌ 金额不能为负数")
                return
        except ValueError:
            await update.message.reply_text("❌ 请输入整数金额")
            return
        await self.account_manager.update_account(name, bet_params={param_name: amount})
        if param_name == 'base_amount':
            await self.account_manager.update_account(name, recommend_mode=False)
        await update.message.reply_text(f"✅ {param_name} 已设置为 {amount} KK")
        await self._show_account_detail(update.message, update.effective_user.id, name)
        context.user_data.pop('amount_name', None)
        context.user_data.pop('amount_param', None)
        return ConversationHandler.END

    async def chase_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        name = query.data.split(':')[1]
        context.user_data['chase_name'] = name
        await query.edit_message_text(
            "🔢 *设置数字追号 - 第1步/共3步*\n\n请输入要追的数字（0-27），多个数字用空格分隔：\n例如：`0 5 12`\n\n点击 /cancel 取消",
            parse_mode='Markdown'
        )
        return Config.CHASE_NUMBERS

    async def chase_input_numbers(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        numbers = []
        for p in re.split(r'[,\s、]+', text):
            p = p.strip()
            if p.isdigit() and 0 <= int(p) <= 27:
                numbers.append(int(p))
        numbers = list(set(numbers))
        if not numbers:
            await update.message.reply_text("❌ 未输入有效数字（0-27），请重新输入：")
            return Config.CHASE_NUMBERS
        context.user_data['chase_numbers'] = numbers
        await update.message.reply_text(
            f"✅ 已记录数字：{', '.join(map(str, numbers))}\n\n"
            "🔢 *第2步/共3步：请输入追号期数*\n\n请输入一个正整数：\n点击 /cancel 取消",
            parse_mode='Markdown'
        )
        return Config.CHASE_PERIODS

    async def chase_input_periods(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        if not text.isdigit() or int(text) <= 0:
            await update.message.reply_text("❌ 期数必须是正整数，请重新输入：")
            return Config.CHASE_PERIODS
        context.user_data['chase_periods'] = int(text)
        await update.message.reply_text(
            f"✅ 已设置期数：{text} 期\n\n"
            "🔢 *第3步/共3步：请输入每注金额*\n\n"
            "输入0则使用基础金额：\n点击 /cancel 取消",
            parse_mode='Markdown'
        )
        return Config.CHASE_AMOUNT

    async def chase_input_amount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        try:
            amount = int(text)
            if amount < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ 请输入有效的整数金额")
            return Config.CHASE_AMOUNT

        name = context.user_data.get('chase_name')
        numbers = context.user_data.get('chase_numbers', [])
        periods = context.user_data.get('chase_periods', 0)
        if not name:
            await update.message.reply_text("❌ 会话已过期")
            return ConversationHandler.END

        await self.account_manager.update_account(name, chase_enabled=True, chase_numbers=numbers,
                                                   chase_periods=periods, chase_current=0, chase_amount=amount)
        await update.message.reply_text(
            f"✅ *追号设置成功！*\n\n数字：{', '.join(map(str, numbers))}\n期数：{periods}\n"
            f"每注：{amount if amount>0 else '基础金额'}KK",
            parse_mode='Markdown'
        )
        await self._show_account_detail(update.message, update.effective_user.id, name)
        context.user_data.pop('chase_name', None)
        context.user_data.pop('chase_numbers', None)
        context.user_data.pop('chase_periods', None)
        return ConversationHandler.END

    async def handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        text = update.message.text.strip()
        # 手动投注格式：类型 金额
        match = re.match(r'^([大小单双大小单双]{1,2})\s+(\d+)$', text)
        if match:
            bet_type = match.group(1)
            amount = int(match.group(2))
            for name, acc in self.account_manager.accounts.items():
                if acc.game_group_id == chat_id:
                    if amount < Config.MIN_BET_AMOUNT or amount > Config.MAX_BET_AMOUNT:
                        await update.message.reply_text(f"❌ 金额必须在{Config.MIN_BET_AMOUNT}-{Config.MAX_BET_AMOUNT}KK之间")
                        return
                    success = await self.message_sender.send_bet_message(chat_id, [f"{bet_type} {amount}"])
                    if success:
                        await update.message.reply_text(f"✅ 已投注: {bet_type} {amount}KK")
                    else:
                        await update.message.reply_text("❌ 投注发送失败")
                    return

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        user = query.from_user.id

        route_map = {
            "menu:main": self._show_main_menu, "menu:prediction": self._show_prediction_menu,
            "menu:status": self._show_status_menu, "menu:help": self._show_help_menu,
            "add_account": self.add_account_start, "run_analysis": self._process_run_analysis,
            "refresh_status": self._show_status_menu, "menu:accounts": self._show_accounts_menu,
        }
        if data in route_map:
            await route_map[data](query)
            return

        if data.startswith("select_account:"):
            await self._show_account_detail(query, user, data.split(":")[1])
        elif data.startswith("action:"):
            parts = data.split(":")
            action = parts[1]
            name = parts[2] if len(parts) > 2 else None
            await self._process_action(query, user, action, name)
        elif data.startswith("amount_menu:"):
            await self._show_amount_menu(query, user, data.split(":")[1])
        elif data.startswith("set_strategy:"):
            _, name, strategy = data.split(":")
            await self._process_set_strategy(query, user, name, strategy)
        elif data.startswith("set_scheme:"):
            _, name, scheme = data.split(":")
            await self._process_set_scheme(query, user, name, scheme)
        elif data.startswith("toggle_content:"):
            await self._toggle_prediction_content(query, user, data.split(":")[1])
        elif data.startswith("clear_streak:"):
            await self._clear_streak_records(query, user, data.split(":")[1])
        elif data.startswith("amount_recommend:"):
            await self._toggle_recommend_mode(query, user, data.split(":")[1])

    async def _show_main_menu(self, query):
        kb = [[InlineKeyboardButton("📱 账户管理", callback_data="menu:accounts")],
              [InlineKeyboardButton("🎯 智能预测", callback_data="menu:prediction")],
              [InlineKeyboardButton("📊 系统状态", callback_data="menu:status")],
              [InlineKeyboardButton("❓ 帮助", callback_data="menu:help")],
              [InlineKeyboardButton("📖 使用手册", url=Config.MANUAL_LINK)]]
        await query.edit_message_text("🎮 *PC28 智能投注系统*\n\nBot直接发送投注消息到群组！",
                                      reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_accounts_menu(self, query, user):
        accounts = self.account_manager.get_user_accounts(user)
        kb = [[InlineKeyboardButton("➕ 添加账户", callback_data="add_account")]]
        for acc in accounts:
            status = "🟢" if acc.auto_betting else "⚪"
            kb.append([InlineKeyboardButton(f"{status} {acc.get_display_name()}", callback_data=f"select_account:{acc.name}")])
        kb.append([InlineKeyboardButton("🔙 返回", callback_data="menu:main")])
        text = "📭 您还没有添加账户" if not accounts else "📱 *您的账户列表*\n\n"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_account_detail(self, query_or_message, user, name):
        acc = self.account_manager.get_account(name)
        if not acc:
            await query_or_message.edit_message_text("❌ 账户不存在")
            return

        status = "🟢 自动投注中" if acc.auto_betting else "⚪ 未投注"
        if acc.prediction_broadcast:
            status += " | 📊 播报中"
        if acc.chase_enabled:
            status += f" | 🔢 追{acc.chase_current}/{acc.chase_periods}"
        if acc.recommend_mode:
            status += f" | 💰 推荐({acc.risk_profile})"

        base_text = "推荐模式" if acc.recommend_mode else f"{acc.bet_params.base_amount} KK"
        content_type = "双组" if acc.prediction_content == "double" else "杀组"
        bet_btn = "🛑 停止自动投注" if acc.auto_betting else "🤖 开启自动投注"
        pred_btn = "🛑 停止播报" if acc.prediction_broadcast else "📊 开启播报"

        kb = [
            [InlineKeyboardButton("💬 游戏群", callback_data=f"action:setgroup:{name}"),
             InlineKeyboardButton("📢 播报群", callback_data=f"action:setpredgroup:{name}")],
            [InlineKeyboardButton("🎯 投注方案", callback_data=f"set_scheme:{name}:select"),
             InlineKeyboardButton("📈 金额策略", callback_data=f"set_strategy:{name}:select")],
            [InlineKeyboardButton("💰 金额设置", callback_data=f"amount_menu:{name}"),
             InlineKeyboardButton("🔢 设置追号", callback_data=f"action:setchase:{name}")],
            [InlineKeyboardButton(f"🎛️ 播报内容({content_type})", callback_data=f"toggle_content:{name}")],
            [InlineKeyboardButton(bet_btn, callback_data=f"action:toggle_bet:{name}"),
             InlineKeyboardButton(pred_btn, callback_data=f"action:toggle_pred:{name}")],
            [InlineKeyboardButton("📊 账户统计", callback_data=f"action:status:{name}")],
            [InlineKeyboardButton("🔙 返回", callback_data="menu:accounts")]
        ]
        if acc.chase_enabled:
            kb.insert(4, [InlineKeyboardButton("🛑 停止追号", callback_data=f"action:stopchase:{name}")])

        text = f"📱 *账户: {acc.get_display_name()}*\n\n状态: {status}\n基础金额: {base_text}\n净盈利: {acc.total_wins*2 - acc.total_bets}K"
        if hasattr(query_or_message, 'edit_message_text'):
            await query_or_message.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        else:
            await query_or_message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_amount_menu(self, query, user, name):
        acc = self.account_manager.get_account(name)
        if not acc:
            return
        recommend_btn = "🛑 停止推荐" if acc.recommend_mode else "🤖 一键推荐"
        text = f"💰 *金额设置 - {acc.get_display_name()}*\n\n当前设置:\n• 基础金额: {acc.bet_params.base_amount} KK\n• 最大金额: {acc.bet_params.max_amount} KK\n• 止损: {acc.bet_params.stop_loss} KK\n• 止盈: {acc.bet_params.stop_win} KK"
        kb = [[InlineKeyboardButton("💰 基础金额", callback_data=f"amount_set:base_amount:{name}"),
               InlineKeyboardButton("💎 最大金额", callback_data=f"amount_set:max_amount:{name}")],
              [InlineKeyboardButton("🛑 停止余额", callback_data=f"amount_set:stop_balance:{name}"),
               InlineKeyboardButton("⛔ 止损金额", callback_data=f"amount_set:stop_loss:{name}")],
              [InlineKeyboardButton("✅ 止盈金额", callback_data=f"amount_set:stop_win:{name}"),
               InlineKeyboardButton("🔄 恢复余额", callback_data=f"amount_set:resume_balance:{name}")],
              [InlineKeyboardButton(recommend_btn, callback_data=f"amount_recommend:{name}")],
              [InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{name}")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _toggle_recommend_mode(self, query, user, name):
        acc = self.account_manager.get_account(name)
        if acc:
            await self.account_manager.update_account(name, recommend_mode=not acc.recommend_mode)
            await query.edit_message_text(f"✅ 已{'启用' if not acc.recommend_mode else '关闭'}推荐模式")
            await self._show_amount_menu(query, user, name)

    async def _toggle_prediction_content(self, query, user, name):
        acc = self.account_manager.get_account(name)
        if acc:
            new_content = "kill" if acc.prediction_content == "double" else "double"
            await self.account_manager.update_account(name, prediction_content=new_content)
            await query.edit_message_text(f"✅ 播报内容已切换为 {'杀组' if new_content=='kill' else '双组'}")
            await self._show_account_detail(query, user, name)

    async def _clear_streak_records(self, query, user, name):
        await query.edit_message_text("✅ 记录已删除")
        await self._show_account_detail(query, user, name)

    async def _process_action(self, query, user, action, name):
        if not name:
            return
        if action == "toggle_bet":
            acc = self.account_manager.get_account(name)
            await self.account_manager.update_account(name, auto_betting=not acc.auto_betting)
            await query.edit_message_text(f"✅ 自动投注已{'开启' if not acc.auto_betting else '关闭'}")
            await self._show_account_detail(query, user, name)
        elif action == "toggle_pred":
            acc = self.account_manager.get_account(name)
            if acc.prediction_broadcast:
                await self.prediction_broadcaster.stop_broadcast(name, user)
            else:
                await self.prediction_broadcaster.start_broadcast(name, user)
            await self._show_account_detail(query, user, name)
        elif action == "stopchase":
            await self.account_manager.update_account(name, chase_enabled=False, chase_stop_reason="手动停止")
            await query.edit_message_text("✅ 追号已停止")
            await self._show_account_detail(query, user, name)
        elif action == "status":
            await self._show_account_status(query, name)
        elif action == "setgroup" or action == "setpredgroup":
            pass

    async def _show_account_status(self, query, name):
        acc = self.account_manager.get_account(name)
        if acc:
            text = f"📊 *账户状态 - {acc.get_display_name()}*\n\n策略: {acc.betting_strategy}\n方案: {acc.betting_scheme}\n总投注: {acc.total_bets}\n命中: {acc.total_wins}\n连赢: {acc.consecutive_wins} | 连输: {acc.consecutive_losses}"
            if acc.chase_enabled:
                text += f"\n\n追号: {acc.chase_current}/{acc.chase_periods}期"
            kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{name}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _process_set_strategy(self, query, user, name, strategy):
        if strategy == "select":
            kb = [[InlineKeyboardButton(s, callback_data=f"set_strategy:{name}:{s}")] for s in ['保守', '平衡', '激进', '马丁格尔', '斐波那契']]
            kb.append([InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{name}")])
            await query.edit_message_text("📊 *选择投注策略:*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
            return
        strategy_config = {'保守': (10000, 100000, 1.5), '平衡': (50000, 500000, 2.0),
                           '激进': (100000, 1000000, 2.5), '马丁格尔': (10000, 10000000, 2.0),
                           '斐波那契': (10000, 10000000, 1.0)}
        base, max_amt, mult = strategy_config.get(strategy, (10000, 100000, 2.0))
        await self.account_manager.update_account(name, betting_strategy=strategy,
                                                   bet_params={'base_amount': base, 'max_amount': max_amt, 'multiplier': mult})
        if strategy in ['保守', '平衡', '激进']:
            await self.account_manager.update_account(name, risk_profile=strategy)
        await query.edit_message_text(f"✅ 已设置为 {strategy} 策略")
        await self._show_account_detail(query, user, name)

    async def _process_set_scheme(self, query, user, name, scheme):
        if scheme == "select":
            kb = [[InlineKeyboardButton(s, callback_data=f"set_scheme:{name}:{s}")] for s in ['组合1', '组合2', '组合1+2', '杀主']]
            kb.append([InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{name}")])
            await query.edit_message_text("🎯 *选择投注方案:*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
            return
        await self.account_manager.update_account(name, betting_scheme=scheme)
        await query.edit_message_text(f"✅ 投注方案已设置为: {scheme}")
        await self._show_account_detail(query, user, name)

    async def _show_prediction_menu(self, query):
        kb = [[InlineKeyboardButton("🔮 运行预测", callback_data="run_analysis")],
              [InlineKeyboardButton("🔙 返回", callback_data="menu:main")]]
        await query.edit_message_text("🎯 *预测分析菜单*", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _process_run_analysis(self, query):
        await query.edit_message_text("🔍 正在生成预测...")
        history = await self.api.get_history(50)
        if len(history) < 3:
            await query.edit_message_text("❌ 历史数据不足")
            return
        latest = history[0]
        keno = await self.api.get_latest_keno()
        if keno:
            self.model.current_keno_nbrs = keno['nbrs']
        pred = self.model.predict(history, latest)
        text = f"🎯 *预测结果*\n\n最新: {latest.get('qihao')} = {latest.get('sum')} ({latest.get('combo')})\n\n主推: {pred['main']}\n候选: {pred['candidate']}\n杀组: {pred['kill']}\n置信度: {pred['confidence']}%"
        kb = [[InlineKeyboardButton("🔄 刷新", callback_data="run_analysis")],
              [InlineKeyboardButton("🔙 返回", callback_data="menu:prediction")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_status_menu(self, query):
        api_stats = self.api.get_statistics()
        sched_stats = self.bet_executor.get_stats()
        total_bets = sum(a.total_bets for a in self.account_manager.accounts.values())
        total_wins = sum(a.total_wins for a in self.account_manager.accounts.values())
        text = f"📊 *系统状态*\n\n缓存数据: {api_stats['缓存数据量']}期\n自动投注: {sched_stats['auto_betting_accounts']}个\n播报中: {sched_stats['broadcast_accounts']}个\n总投注: {total_bets}\n总命中: {total_wins}\n净盈利: {total_wins*2 - total_bets}K"
        kb = [[InlineKeyboardButton("🔄 刷新", callback_data="refresh_status")],
              [InlineKeyboardButton("🔙 返回", callback_data="menu:main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_help_menu(self, query):
        text = "📚 *帮助菜单*\n\n1. 添加账户：账户管理 -> 添加账户\n2. 设置群组：账户详情 -> 游戏群/播报群\n3. 开启自动投注：账户详情 -> 开启自动投注\n4. 手动投注：在游戏群发送 `大 10000`\n\n获取群ID：添加 @userinfobot 到群组获取"
        kb = [[InlineKeyboardButton("🔙 返回", callback_data="menu:main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')


# ==================== 启动 ====================
async def post_init(application):
    bot = application.bot_data.get('bot')
    if bot:
        await bot.account_manager.start_periodic_save()
        await bot.global_scheduler.start()
    logger.log_system("Bot 启动完成")


def main():
    def handle_shutdown(signum, frame):
        print("\n🛑 正在关闭...")
        if 'bot' in globals():
            asyncio.create_task(bot.global_scheduler.stop())
            asyncio.create_task(bot.account_manager.stop_periodic_save())
            asyncio.create_task(bot.api.close())
        print("✅ 已安全关闭")
        exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("""
========================================
PC28 智能预测投注系统（Bot直发版）
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
    bot.application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    random.seed(time.time())
    np.random.seed(int(time.time()))
    main()