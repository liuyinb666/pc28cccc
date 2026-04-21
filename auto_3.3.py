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
from typing import Optional, Dict, List, Any, Tuple, Union, Set
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
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# ==================== 配置 ====================
class Config:
    # 从环境变量读取敏感信息
    BOT_TOKEN = os.environ.get('BOT_TOKEN', '8723628059:AAEICW5iWSoueLZP-pzjp7ytOuyKST7lU70')
    API_ID = int(os.environ.get('API_ID', '2040'))
    API_HASH = os.environ.get('API_HASH', 'b18441a1ff607e10a989891a5462e627')
    PC28_API_BASE = "https://www.pc28.help/api/kj.json?nbr=200"
    ADMIN_USER_IDS = [int(id) for id in os.environ.get('ADMIN_USER_IDS', '7673012566').split(',') if id.strip()]
    
    # 硅基流动API配置
    SILICONFLOW_API_KEY = os.environ.get('SILICONFLOW_API_KEY', 'sk-vipzurajvbmxqdnqffipqcfvfuquklhyudcwarjhqyitjpcp')
    SILICONFLOW_MODEL = os.environ.get('SILICONFLOW_MODEL', "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B")
    
    # 数据目录（支持Railway持久化卷）
    RAILWAY_VOLUME = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '')
    if RAILWAY_VOLUME:
        BASE_DIR = Path(RAILWAY_VOLUME)
    else:
        BASE_DIR = Path(os.environ.get('DATA_DIR', '.'))
    
    DATA_DIR = BASE_DIR / "data"
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
    EXPLORATION_RATE = 0.03
    EXPLORATION_MIN = 0.005
    EXPLORATION_DECAY = 0.95
    NOISE_SCALE = 0.05
    MODEL_SAVE_FILE = str(DATA_DIR / "pc28_model.json")
    BALANCE_CACHE_SECONDS = 60
    MAX_CONCURRENT_BETS = 3
    LOG_RETENTION_DAYS = 7
    ACCOUNT_SAVE_INTERVAL = 30
    MAX_CONCURRENT_PREDICTIONS = 1
    LOGIN_SELECT, LOGIN_CODE, LOGIN_PASSWORD = range(3)
    ADD_ACCOUNT = 10
    CHASE_NUMBERS, CHASE_PERIODS, CHASE_AMOUNT = range(11, 14)
    MAX_ACCOUNTS_PER_USER = 5
    KJ_HISTORY_DOWNLOAD = 1000

    @classmethod
    def init_dirs(cls):
        cls.DATA_DIR.mkdir(exist_ok=True, parents=True)
        cls.SESSIONS_DIR.mkdir(exist_ok=True, parents=True)
        cls.LOGS_DIR.mkdir(exist_ok=True, parents=True)
        cls.CACHE_DIR.mkdir(exist_ok=True, parents=True)

    @classmethod
    def validate(cls):
        errors = []
        if not cls.BOT_TOKEN: errors.append("BOT_TOKEN未配置")
        if cls.API_ID <= 0: errors.append("API_ID必须为正整数")
        if not cls.API_HASH: errors.append("API_HASH未配置")
        if not cls.PC28_API_BASE.startswith(('http://', 'https://')): errors.append("PC28_API_BASE必须是有效的URL")
        if cls.MIN_BET_AMOUNT < 0: errors.append("最小投注金额不能为负数")
        if cls.MAX_BET_AMOUNT <= cls.MIN_BET_AMOUNT: errors.append("最大投注金额必须大于最小投注金额")
        if cls.MAX_CONCURRENT_BETS < 1: errors.append("并发投注数至少为1")
        if errors: raise ValueError("配置验证失败: " + ", ".join(errors))
        return True

Config.init_dirs()

# ==================== 工具函数 ====================
def increment_qihao(current_qihao: str) -> str:
    if not current_qihao: return "1"
    match = re.search(r'(\d+)$', current_qihao)
    if match:
        num_part = match.group(1)
        prefix = current_qihao[:match.start()]
        try:
            next_num = str(int(num_part) + 1).zfill(len(num_part))
            return prefix + next_num
        except: return current_qihao + "1"
    else:
        try: return str(int(current_qihao) + 1)
        except: return current_qihao + "1"

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
        if hasattr(record, 'betting') and record.betting: self._style._fmt = self.FORMATS['BETTING']
        elif hasattr(record, 'prediction') and record.prediction: self._style._fmt = self.FORMATS['PREDICTION']
        else: self._style._fmt = self.FORMATS.get(record.levelno, self.grey + "%(asctime)s [%(levelname)s] %(message)s" + self.reset)
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
                if (now - file_date).days > Config.LOG_RETENTION_DAYS: f.unlink()
            except: pass

    def log_system(self, msg): self.logger.info(f"[系统] {msg}")
    def log_account(self, user_id, phone, action): self.logger.info(f"[账户] 用户:{user_id} 手机:{self._mask_phone(phone)} {action}")
    def log_game(self, msg): self.logger.info(f"[游戏] {msg}")
    def log_betting(self, user_id, action, detail):
        extra = {'betting': True}; self.logger.info(f"用户:{user_id} {action} {detail}", extra=extra)
    def log_prediction(self, user_id, action, detail):
        extra = {'prediction': True}; self.logger.info(f"用户:{user_id} {action} {detail}", extra=extra)
    def log_analysis(self, msg): self.logger.debug(f"[分析] {msg}")
    def log_error(self, user_id, action, error):
        error_trace = traceback.format_exc(); self.logger.error(f"[错误] 用户:{user_id} {action}: {error}\n{error_trace}")
    def log_api(self, action, detail): self.logger.debug(f"[API] {action} {detail}")
    def _mask_phone(self, phone: str) -> str:
        if len(phone) >= 8: return phone[:5] + "****" + phone[-3:]
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

# ==================== PC28规则预测器 ====================
class PC28RulePredictor:
    def __init__(self):
        self.combos = COMBOS
        self.size_map = {"小": "大", "大": "小"}
        self.parity_map = {"单": "双", "双": "单"}
        
        self.order_3y_map = {
            0: ['3Y0', '3Y1', '3Y2'],
            1: ['3Y1', '3Y2', '3Y0'],
            2: ['3Y2', '3Y0', '3Y1'],
        }
        
        self.pool_5y = {
            0: [0, 5, 10, 15, 20, 25],
            1: [1, 6, 11, 16, 21],
            2: [2, 7, 12, 17, 22],
            3: [3, 8, 13, 18, 23],
            4: [4, 9, 14, 19, 24],
        }
        
        self.pool_3y = {
            0: [0, 3, 6, 9, 12, 15, 18, 21, 24, 27],
            1: [1, 4, 7, 10, 13, 16, 19, 22, 25],
            2: [2, 5, 8, 11, 14, 17, 20, 23, 26],
        }
        
        self.high_freq_combos = ["小单", "小双", "大双"]
    
    def _calc_y_value(self, a: int, b: int, c: int, total: int) -> int:
        concat_num = a * 100 + b * 10 + c
        new_num = concat_num + total
        return sum(int(d) for d in str(new_num))
    
    def _calc_3y(self, total: int) -> int:
        return total % 3
    
    def _calc_5y(self, total: int) -> int:
        return total % 5
    
    def _calc_kill_by_sub_algo1(self, latest: Dict, history_10: List[Dict]) -> Optional[str]:
        try:
            a1 = latest.get('a', 0)
            b1 = latest.get('b', 0)
            c1 = latest.get('c', 0)
            h1 = latest.get('sum', 0)
            
            y1 = self._calc_y_value(a1, b1, c1, h1)
            
            target_idx = -1
            for i, h in enumerate(history_10):
                if h.get('y_value') == y1:
                    target_idx = i
                    break
            
            if target_idx == -1:
                return None
            
            target = history_10[target_idx]
            a2 = target.get('a', 0)
            b2 = target.get('b', 0)
            c2 = target.get('c', 0)
            
            s = abs(a1 - a2) + abs(b1 - b2) + abs(c1 - c2)
            
            size = "小" if s < 14 else "大"
            parity = "单" if s % 2 == 1 else "双"
            
            return size + parity
        except Exception as e:
            logger.log_error(0, "子算法1计算失败", e)
            return None
    
    def _calc_kill_by_sub_algo2(self, latest: Dict) -> Optional[str]:
        try:
            h1 = latest.get('sum', 0)
            a1 = latest.get('a', 0)
            
            step1_raw = h1 * 3 * h1
            step1_str = str(step1_raw)[-3:].zfill(3)
            d2 = sum(int(d) for d in step1_str)
            
            s = d2 + a1
            if s > 27:
                s = s - 27
            
            size = "小" if s < 14 else "大"
            parity = "单" if s % 2 == 1 else "双"
            temp_combo = size + parity
            
            opposite_size = self.size_map[size]
            opposite_parity = self.parity_map[parity]
            return opposite_size + opposite_parity
        except Exception as e:
            logger.log_error(0, "子算法2计算失败", e)
            return None
    
    def get_rule_based_kill(self, latest: Dict, history_10: List[Dict]) -> Tuple[Optional[str], int]:
        kill1 = self._calc_kill_by_sub_algo1(latest, history_10)
        kill2 = self._calc_kill_by_sub_algo2(latest)
        
        if kill1 and kill2 and kill1 == kill2:
            logger.log_analysis(f"双算法杀组一致: {kill1}")
            return kill1, 90
        elif kill2:
            logger.log_analysis(f"杀组不一致: 算法1={kill1}, 算法2={kill2}, 采用算法2")
            return kill2, 70
        elif kill1:
            return kill1, 60
        return None, 0
    
    def _calculate_tail_numbers(self, history_10: List[Dict], latest: Dict) -> Tuple[List[int], str]:
        h1 = latest.get('sum', 0)
        y3 = self._calc_3y(h1)
        
        order = self.order_3y_map.get(y3, ['3Y0', '3Y1', '3Y2'])
        
        pools_3y = {'3Y0': [], '3Y1': [], '3Y2': []}
        for h in history_10[:10]:
            total = h.get('sum', 0)
            y3_val = self._calc_3y(total)
            pool_key = f'3Y{y3_val}'
            pools_3y[pool_key].append(h)
        
        ball_map = {'3Y0': 'a', '3Y1': 'b', '3Y2': 'c'}
        
        tail_sums = []
        for pool_key in order:
            pool = pools_3y.get(pool_key, [])
            ball = ball_map.get(pool_key, 'a')
            
            if len(pool) >= 3:
                values = [h.get(ball, 0) for h in pool[:3]]
                tail_sum = sum(values) % 10
            else:
                tail_sum = 0
            tail_sums.append(tail_sum)
        
        h2 = sum(tail_sums) % 10
        
        base_size = "小" if h2 < 14 else "大"
        base_parity = "单" if h2 % 2 == 1 else "双"
        base_combo = base_size + base_parity
        
        return tail_sums, base_combo
    
    def _calculate_scores(self, history_10: List[Dict], base_combo: str, rule_kill: str) -> Dict[str, Dict]:
        combo_count = {c: 0 for c in self.combos}
        for h in history_10[:10]:
            combo = h.get('combo', '')
            if combo in combo_count:
                combo_count[combo] += 1
        
        scores = {}
        for combo in self.combos:
            if combo == rule_kill:
                scores[combo] = {'score': 0, 'tail_score': 0, 'freq_score': 0, 'is_kill': True}
                continue
            
            if combo == base_combo:
                tail_score = 40
            elif combo[0] == base_combo[0] or combo[1] == base_combo[1]:
                tail_score = 30
            else:
                tail_score = 10
            
            count = combo_count.get(combo, 0)
            if count >= 3:
                freq_score = 60
            elif count >= 1:
                freq_score = 40
            else:
                freq_score = 10
            
            if combo in self.high_freq_combos:
                freq_score += 10
            
            total_score = tail_score + freq_score
            
            scores[combo] = {
                'score': total_score,
                'tail_score': tail_score,
                'freq_score': freq_score,
                'count': count,
                'is_kill': False
            }
        
        return scores
    
    def get_rule_based_predictions(self, history_10: List[Dict]) -> Dict:
        if len(history_10) < 10:
            logger.log_analysis(f"历史数据不足10期，当前{len(history_10)}期")
            return None
        
        latest = history_10[0]
        
        for h in history_10:
            if 'y_value' not in h and h.get('a') is not None:
                h['y_value'] = self._calc_y_value(
                    h.get('a', 0), h.get('b', 0), h.get('c', 0), h.get('sum', 0)
                )
        
        rule_kill, kill_conf = self.get_rule_based_kill(latest, history_10)
        if not rule_kill:
            logger.log_analysis("杀组计算失败，使用默认杀组")
            rule_kill = random.choice(self.combos)
            kill_conf = 50
        
        tail_sums, base_combo = self._calculate_tail_numbers(history_10, latest)
        
        scores = self._calculate_scores(history_10, base_combo, rule_kill)
        
        sorted_combos = sorted(
            [(c, data) for c, data in scores.items() if not data.get('is_kill')],
            key=lambda x: x[1]['score'],
            reverse=True
        )
        
        main_combos = []
        candidate_combo = None
        
        for combo, data in sorted_combos:
            score = data['score']
            if score >= 90 and len(main_combos) < 2:
                main_combos.append(combo)
            elif 80 <= score < 90 and candidate_combo is None:
                candidate_combo = combo
        
        if len(main_combos) < 2 and candidate_combo:
            main_combos.append(candidate_combo)
            candidate_combo = None
        
        for combo, data in sorted_combos:
            if combo not in main_combos and combo != candidate_combo and len(main_combos) < 2:
                main_combos.append(combo)
        
        h1 = latest.get('sum', 0)
        y5 = self._calc_5y(h1)
        y3 = self._calc_3y(h1)
        
        pool_5y = self.pool_5y.get(y5, [0, 5, 10, 15, 20, 25])
        pool_3y = self.pool_3y.get(y3, [])
        
        intersection = [n for n in pool_5y if n in pool_3y]
        
        sum_count = Counter()
        for h in history_10[:10]:
            s = h.get('sum', 0)
            sum_count[s] += 1
        
        remaining = []
        for num in pool_5y:
            if num not in intersection:
                remaining.append((num, sum_count.get(num, 0)))
        remaining.sort(key=lambda x: x[1], reverse=True)
        
        special_numbers = intersection.copy()
        for num, _ in remaining:
            if len(special_numbers) >= 4:
                break
            special_numbers.append(num)
        
        while len(special_numbers) < 4:
            special_numbers.append(random.randint(0, 27))
        
        jump_risk = f"双算法一致判定杀[{rule_kill}]，{rule_kill}近期无连续走势/和值支撑回补，极低等级跳开风险；核心圈（{','.join(main_combos)}）全覆盖近期高概率开出方向"
        
        main_scores = [scores.get(c, {}).get('score', 0) for c in main_combos]
        avg_main_score = sum(main_scores) / len(main_scores) if main_scores else 0
        confidence = min(95, int(avg_main_score * 0.8 + kill_conf * 0.2))
        
        result = {
            'main': main_combos,
            'candidate': candidate_combo if candidate_combo else main_combos[-1] if main_combos else self.combos[0],
            'kill': rule_kill,
            'kill_confidence': kill_conf,
            'confidence': confidence,
            'special_numbers': special_numbers[:4],
            'jump_risk': jump_risk,
            'base_combo': base_combo,
            'tail_sums': tail_sums,
            'scores': {c: data['score'] for c, data in scores.items()},
            'algo_details': [
                {"name": "双杀组算法", "kill": rule_kill, "confidence": kill_conf},
                {"name": "双Y融合算法", "main": main_combos, "candidate": candidate_combo},
                {"name": "5Y+3Y特码池", "numbers": special_numbers[:4]}
            ]
        }
        
        logger.log_prediction(0, "规则预测完成", 
                            f"主推:{main_combos} 候选:{candidate_combo} 杀组:{rule_kill} 置信度:{confidence}")
        
        return result

# ==================== 硅基流动AI客户端 ====================
class SiliconFlowAIClient:
    def __init__(self, api_key=None, model_name="deepseek-ai/DeepSeek-R1-Distill-Qwen-7B"):
        self.api_url = "https://api.siliconflow.cn/v1/chat/completions"
        self.api_key = api_key or Config.SILICONFLOW_API_KEY
        self.model_name = model_name
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=25)
        self._active_requests = {}
        self._global_predict_lock = asyncio.Lock()
        self._last_prediction = None
        self._last_qihao = None
        self._last_prompt_hash = None
        
        self.rule_predictor = PC28RulePredictor()
    
    def _build_rule_based_prompt(self, history: List[Dict], rule_result: Dict) -> str:
        if not rule_result:
            return self._build_fallback_prompt(history)
        
        combos_10 = [h.get('combo', '') for h in history[:10] if h.get('combo')]
        sums_10 = [h.get('sum', 0) for h in history[:10] if h.get('sum') is not None]
        
        combo_count = Counter(combos_10)
        
        current_streak = 1
        if combos_10:
            for i in range(1, len(combos_10)):
                if combos_10[i] == combos_10[0]:
                    current_streak += 1
                else:
                    break
        
        prompt = f"""你是PC28彩票预测验证专家。以下是基于确定性规则算法的预测结果，请验证其合理性。

【规则算法预测结果】
- 最终杀组（必排除）：{rule_result['kill']}（置信度{rule_result['kill_confidence']}%）
- 核心主攻组合：{rule_result['main']}
- 高概率稳防组合：{rule_result['candidate']}
- 预测置信度：{rule_result['confidence']}%
- 核心特码：{rule_result['special_numbers']}

【近期走势数据（最近10期）】
- 组合序列：{" → ".join(combos_10[:10])}
- 和值序列：{sums_10[:10]}
- 当前连开：{current_streak}期（{combos_10[0] if combos_10 else '无'}）
- 组合频次：{dict(combo_count)}

【规则算法说明】
1. 杀组由双算法一致判定（Y值位差和算法 + 和值运算对立算法）
2. 主攻组合由双Y融合+双权重打分选出（尾数和40% + 走势频次60%）
3. 特码由5Y+3Y池交集补充高频热码得出

【验证任务】
请基于PC28开奖规律，验证上述规则预测是否合理，并输出JSON格式结果：

{{
    "validation": "合理/需调整/不合理",
    "main_confirm": [确认的主攻组合，保持原顺序],
    "candidate_confirm": "确认的稳防组合",
    "kill_confirm": "确认的杀组",
    "adjustment_reason": "调整原因（如无需调整填'无'）",
    "final_confidence": 整数置信度(40-95)
}}

注意：
- 如果杀组近期出现频次≥2次，考虑调整
- 如果主攻组合连续3期未出，考虑加强
- 如果当前连开≥3期，考虑反转
- 保持输出简洁，仅输出JSON"""
        
        return prompt
    
    def _build_fallback_prompt(self, history: List[Dict]) -> str:
        combos_10 = [h.get('combo', '') for h in history[:10] if h.get('combo')]
        sums_10 = [h.get('sum', 0) for h in history[:10] if h.get('sum') is not None]
        
        combo_count = Counter(combos_10)
        
        prompt = f"""你是PC28彩票预测专家。基于以下数据预测下一期。

【最近10期数据】
组合序列：{" → ".join(combos_10[:10])}
和值序列：{sums_10[:10]}
组合频次：{dict(combo_count)}

【预测规则优先级】
1. 连开≥3期强制排除该组合
2. 短期冷号（10期出现≤1次）优先预测
3. 大小/单双走势一致性≥80%时预测反转

【输出格式】
仅输出JSON：{{"main":"组合","candidate":"组合","kill":"组合","confidence":整数}}
可选组合：小单、小双、大单、大双
置信度范围：40-90"""
        
        return prompt
    
    def _parse_ai_response(self, text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
        try:
            start = text.find('{')
            end = text.rfind('}') + 1
            if start == -1 or end == 0:
                return None, None, None, None
            
            json_str = text[start:end]
            data = json.loads(json_str)
            
            main = data.get("main") or data.get("main_confirm")
            candidate = data.get("candidate") or data.get("candidate_confirm")
            kill = data.get("kill") or data.get("kill_confirm")
            confidence = data.get("confidence") or data.get("final_confidence")
            
            if not main or not candidate or not kill:
                return None, None, None, None
            
            if main not in COMBOS or candidate not in COMBOS or kill not in COMBOS:
                return None, None, None, None
            
            if main == candidate or main == kill or candidate == kill:
                return None, None, None, None
            
            if not isinstance(confidence, (int, float)):
                confidence = 60
            
            return main, candidate, kill, int(confidence)
        except Exception as e:
            logger.log_error(0, "解析AI响应失败", e)
            return None, None, None, None
    
    async def predict(self, history: List[Dict], qihao: str = None) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
        if qihao and self._last_qihao == qihao and self._last_prediction:
            logger.log_api("使用缓存的预测结果", f"期号: {qihao}")
            return self._last_prediction
        
        async with self._global_predict_lock:
            if qihao and self._last_qihao == qihao and self._last_prediction:
                return self._last_prediction
            
            rule_predictor = PC28RulePredictor()
            rule_result = rule_predictor.get_rule_based_predictions(list(history)[:10])
            
            if rule_result:
                logger.log_analysis(f"规则算法计算结果: 主推{rule_result['main']}, 杀组{rule_result['kill']}")
                
                prompt = self._build_rule_based_prompt(history, rule_result)
                
                ai_result = await self._call_ai_with_prompt(prompt, qihao, history)
                
                if ai_result[0] is not None:
                    logger.log_prediction(0, "AI验证规则结果", f"AI确认: 主推{ai_result[0]}, 杀组{ai_result[2]}")
                    return ai_result
                else:
                    main = rule_result['main'][0] if rule_result['main'] else rule_result['candidate']
                    candidate = rule_result['candidate'] if rule_result['candidate'] else rule_result['main'][-1] if rule_result['main'] else COMBOS[0]
                    kill = rule_result['kill']
                    confidence = rule_result['confidence']
                    logger.log_analysis(f"AI验证失败，使用规则结果: 主推{main}, 候选{candidate}, 杀组{kill}")
                    return main, candidate, kill, confidence
            else:
                logger.log_analysis("规则计算失败，回退到纯AI预测")
                prompt = self._build_fallback_prompt(history)
                return await self._call_ai_with_prompt(prompt, qihao, history)
    
    async def _call_ai_with_prompt(self, prompt: str, qihao: str, history: List[Dict]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
        prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
        
        if prompt_hash in self._active_requests:
            logger.log_api("检测到重复请求", f"等待已有请求完成，hash={prompt_hash[:8]}")
            try:
                result = await asyncio.wait_for(self._active_requests[prompt_hash], timeout=60)
                if qihao:
                    self._last_qihao = qihao
                    self._last_prediction = result
                return result
            except asyncio.TimeoutError:
                del self._active_requests[prompt_hash]
        
        payload = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 256,
            "stream": False
        }
        
        request_task = asyncio.create_task(self._do_predict(payload, prompt_hash, history))
        self._active_requests[prompt_hash] = request_task
        
        try:
            result = await request_task
            if qihao:
                self._last_qihao = qihao
                self._last_prediction = result
            return result
        finally:
            if prompt_hash in self._active_requests:
                del self._active_requests[prompt_hash]
    
    async def _do_predict(self, payload: Dict, prompt_hash: str, history: List[Dict]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
        max_retries = 3
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
                
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.post(self.api_url, headers=headers, json=payload) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            response_text = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                            
                            parsed = self._parse_ai_response(response_text)
                            if parsed[0] is not None:
                                logger.log_api("AI请求成功", f"第{attempt+1}次尝试成功")
                                return parsed
                        elif resp.status == 429:
                            if attempt < max_retries - 1:
                                wait_time = base_delay * (2 ** attempt) * 2
                                await asyncio.sleep(wait_time)
                        else:
                            if attempt < max_retries - 1:
                                await asyncio.sleep(base_delay * (2 ** attempt))
            except asyncio.TimeoutError:
                if attempt == max_retries - 1:
                    return self._get_fallback_from_rule(history)
                await asyncio.sleep(base_delay * (2 ** attempt))
            except Exception as e:
                logger.log_error(0, "AI请求异常", e)
                if attempt == max_retries - 1:
                    return self._get_fallback_from_rule(history)
                await asyncio.sleep(base_delay)
        
        return self._get_fallback_from_rule(history)
    
    def _get_fallback_from_rule(self, history: List[Dict]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
        rule_predictor = PC28RulePredictor()
        rule_result = rule_predictor.get_rule_based_predictions(list(history)[:10])
        
        if rule_result:
            main = rule_result['main'][0] if rule_result['main'] else rule_result['candidate']
            candidate = rule_result['candidate'] if rule_result['candidate'] else main
            kill = rule_result['kill']
            confidence = rule_result['confidence']
            return main, candidate, kill, confidence
        
        combos_20 = [h['combo'] for h in history[:20] if h.get('combo')]
        if combos_20:
            freq = Counter(combos_20)
            main = min(freq, key=freq.get)
            candidates = [c for c in COMBOS if c != main]
            candidate = random.choice(candidates)
            kill = max(freq, key=freq.get)
            return main, candidate, kill, 55
        
        main = random.choice(COMBOS)
        candidate = random.choice([c for c in COMBOS if c != main])
        kill = random.choice([c for c in COMBOS if c != main and c != candidate])
        return main, candidate, kill, 50

# ==================== 模型管理器 ====================
class ModelManager:
    def __init__(self):
        self.ai_client = SiliconFlowAIClient(
            api_key=Config.SILICONFLOW_API_KEY,
            model_name=Config.SILICONFLOW_MODEL
        )
        self.prediction_history = []
        self.recent_accuracy = deque(maxlen=50)
        self._last_ai_kill = None
        self._save_lock = asyncio.Lock()
        self._predict_lock = asyncio.Lock()
        self._last_predict_result = None
        self._last_predict_qihao = None
        
        self.rule_predictor = PC28RulePredictor()

    async def save(self):
        async with self._save_lock:
            try:
                data = {
                    'history': self.prediction_history[-100:],
                    'last_save': datetime.now().isoformat()
                }
                async with aiofiles.open(Config.MODEL_SAVE_FILE, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            except Exception as e:
                logger.log_error(0, "保存预测历史失败", e)

    async def predict(self, history: List[Dict], latest: Dict = None) -> Dict:
        qihao = latest.get('qihao') if latest else None
        
        if qihao and self._last_predict_qihao == qihao and self._last_predict_result:
            logger.log_prediction(0, "使用缓存的预测结果", f"期号: {qihao}")
            return self._last_predict_result

        async with self._predict_lock:
            if qihao and self._last_predict_qihao == qihao and self._last_predict_result:
                return self._last_predict_result

            main, candidate, kill, confidence = await self.ai_client.predict(list(history), qihao)
            
            if main is None:
                rule_result = self.rule_predictor.get_rule_based_predictions(list(history)[:10])
                if rule_result:
                    main = rule_result['main'][0] if rule_result['main'] else rule_result['candidate']
                    candidate = rule_result['candidate'] if rule_result['candidate'] else main
                    kill = rule_result['kill']
                    confidence = rule_result['confidence']
                else:
                    main = random.choice(COMBOS)
                    candidate = random.choice([c for c in COMBOS if c != main])
                    kill = random.choice([c for c in COMBOS if c != main and c != candidate])
                    confidence = 50
                logger.log_analysis("使用规则算法兜底")
            
            if main == candidate:
                candidate = random.choice([c for c in COMBOS if c != main])
            if main == kill or candidate == kill:
                kill = random.choice([c for c in COMBOS if c != main and c != candidate])
            
            result = {
                "main": main,
                "candidate": candidate,
                "kill": kill,
                "confidence": min(95, max(40, confidence)),
                "algo_details": [
                    {"name": "PC28双杀组+双Y融合算法", "kill": kill},
                    {"name": "硅基流动AI验证", "result": f"主推{main}"}
                ],
                "trend_analysis": {}
            }

            if qihao:
                self._last_predict_qihao = qihao
                self._last_predict_result = result

            return result

    async def learn(self, prediction: Dict, actual: str, qihao: str, sum_val: int):
        is_correct = (actual == prediction['main'] or actual == prediction['candidate'])
        record = {
            "time": datetime.now().isoformat(),
            "qihao": qihao,
            "main": prediction['main'],
            "candidate": prediction['candidate'],
            "kill": prediction.get('kill'),
            "actual": actual,
            "sum": sum_val,
            "correct": is_correct
        }
        self.prediction_history.append(record)
        self.recent_accuracy.append(1 if is_correct else 0)

        if len(self.prediction_history) % 10 == 0:
            asyncio.create_task(self.save())

    def get_accuracy_stats(self):
        recent = sum(self.recent_accuracy) / len(self.recent_accuracy) if self.recent_accuracy else 0
        total = sum(1 for r in self.prediction_history if r.get('correct', False)) / len(self.prediction_history) if self.prediction_history else 0
        return {
            'overall': {'recent': recent, 'total': total},
            'algorithms': {'PC28双杀组+双Y融合算法': recent}
        }

    def clear_history(self):
        self.prediction_history = []
        self.recent_accuracy.clear()
        asyncio.create_task(self.save())

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
        self.history_cache = deque(maxlen=Config.CACHE_SIZE)
        self.load_cache()
        logger.log_system("异步API模块初始化完成")

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT))

    def load_cache(self):
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'rb') as f: self.history_cache.extend(pickle.load(f)[:Config.CACHE_SIZE])
        except Exception as e: logger.log_error(0, "加载缓存失败", e)

    def save_cache(self):
        try:
            with open(self.cache_file, 'wb') as f: pickle.dump(list(self.history_cache), f)
        except Exception as e: logger.log_error(0, "保存缓存失败", e)

    async def _make_api_call(self, endpoint, params=None):
        await self.ensure_session()
        for retry in range(Config.MAX_RETRIES):
            self.call_stats['total_calls'] += 1
            start = time.time()
            try:
                url = f"{self.base_url}/{endpoint}.json"
                if params: url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
                logger.log_api("请求", f"GET {url}")
                async with self.session.get(url) as resp:
                    resp.raise_for_status()
                    try:
                        data = await resp.json()
                    except json.JSONDecodeError as e:
                        logger.log_error(0, f"JSON解析失败 {endpoint}", e)
                        text = await resp.text()
                        logger.log_api("原始响应", text[:200])
                        if retry < Config.MAX_RETRIES-1:
                            await asyncio.sleep(Config.RETRY_BACKOFF ** retry)
                            continue
                        else:
                            self.call_stats['failed_calls'] += 1
                            return None
                    if data.get('message') != 'success':
                        logger.log_api("错误", f"API返回非success状态: {data}")
                        self.call_stats['failed_calls'] += 1
                        if retry < Config.MAX_RETRIES-1:
                            await asyncio.sleep(Config.RETRY_BACKOFF ** retry)
                            continue
                        else: return None
                    elapsed = time.time() - start
                    self.call_stats['successful_calls'] += 1
                    self.call_stats['response_times'].append(elapsed)
                    self.call_stats['last_call_time'] = datetime.now()
                    self.call_stats['last_success_time'] = datetime.now()
                    logger.log_api("调用成功", f"{endpoint} 耗时 {elapsed:.2f}秒")
                    return data.get('data', [])
            except asyncio.TimeoutError:
                logger.log_api("超时", f"{endpoint}")
                if retry < Config.MAX_RETRIES-1:
                    await asyncio.sleep(Config.RETRY_BACKOFF ** retry)
                else:
                    self.call_stats['failed_calls'] += 1
                    return None
            except aiohttp.ClientError as e:
                logger.log_api("请求异常", f"{endpoint}: {str(e)}")
                if retry < Config.MAX_RETRIES-1:
                    await asyncio.sleep(Config.RETRY_BACKOFF ** retry)
                else:
                    self.call_stats['failed_calls'] += 1
                    return None
            except Exception as e:
                logger.log_error(0, f"API调用异常 {endpoint}", e)
                if retry < Config.MAX_RETRIES-1:
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
                if text.startswith('\ufeff'): text = text[1:]
                reader = csv.DictReader(StringIO(text))
                rows = [{k.strip(): v.strip() for k, v in row.items()} for row in reader]
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
            a = b = c = 0
            if '+' in number_str:
                parts = number_str.split('+')
                if len(parts) == 3:
                    a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
                    total = a + b + c
            
            if combo and len(combo) >= 2:
                size, parity = combo[0], combo[1]
            elif total is not None:
                size = "大" if total >= 14 else "小"
                parity = "单" if total % 2 else "双"
                combo = size + parity
            else: 
                return None
            
            return {
                'qihao': qihao, 'opentime': f"{date_str} {time_str}", 'opennum': str(total) if total else '',
                'sum': total, 'size': size, 'parity': parity, 'combo': combo,
                'a': a, 'b': b, 'c': c,
                'parsed_time': self._parse_time(date_str, time_str),
                'fetch_time': datetime.now().isoformat(),
                'hash': hashlib.md5(f"{qihao}_{total}".encode()).hexdigest()[:8]
            }
        except Exception as e:
            logger.log_error(0, "解析开奖CSV行失败", e)
            return None

    async def fetch_kj(self, nbr=1):
        data = await self._make_api_call('kj', {'nbr': nbr})
        if not data: 
            return []
        
        processed = []
        for item in data:
            try:
                qihao = str(item.get('nbr', '')).strip()
                if not qihao: 
                    continue
                
                number = item.get('number') or item.get('num')
                if number is None: 
                    continue
                
                a = b = c = total = 0
                if isinstance(number, str) and '+' in number:
                    parts = number.split('+')
                    if len(parts) == 3:
                        a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
                        total = a + b + c
                else:
                    try: 
                        total = int(number)
                    except: 
                        continue
                
                combo = item.get('combination', '')
                if combo and len(combo) >= 2:
                    size, parity = combo[0], combo[1]
                else:
                    size = "大" if total >= 14 else "小"
                    parity = "单" if total % 2 else "双"
                    combo = size + parity
                
                date_str = item.get('date', '')
                time_str = item.get('time', '')
                
                processed.append({
                    'qihao': qihao, 'opentime': f"{date_str} {time_str}", 'opennum': str(total),
                    'sum': total, 'size': size, 'parity': parity, 'combo': combo,
                    'a': a, 'b': b, 'c': c,
                    'parsed_time': self._parse_time(date_str, time_str),
                    'fetch_time': datetime.now().isoformat(),
                    'hash': hashlib.md5(f"{qihao}_{total}".encode()).hexdigest()[:8]
                })
            except Exception as e: 
                logger.log_error(0, f"处理开奖数据项失败", e)
        
        processed.sort(key=lambda x: x.get('parsed_time', datetime.now()), reverse=True)
        logger.log_api("fetch_kj", f"获取到 {len(processed)} 条有效数据")
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
                logger.log_error(0, "初始化失败", "无法获取开奖数据")
                continue
            logger.log_system(f"测试获取到一期数据: {test_data[0]}")
            if len(self.history_cache) >= 50: 
                return True
            kj_data = await self.fetch_kj(nbr=count)
            if not kj_data: 
                continue
            kj_data.sort(key=lambda x: x.get('parsed_time', datetime.now()), reverse=True)
            self.history_cache.clear()
            for item in kj_data:
                if not any(x.get('qihao') == item['qihao'] for x in self.history_cache):
                    self.history_cache.append(item)
            self.save_cache()
            logger.log_system(f"历史数据初始化完成: 开奖 {len(self.history_cache)}条")
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

    async def get_history(self, count=50):
        return list(self.history_cache)[:count]

    async def close(self):
        if self.session and not self.session.closed: 
            await self.session.close()

    def get_statistics(self):
        avg = np.mean(self.call_stats['response_times']) if self.call_stats['response_times'] else 0
        success_rate = (self.call_stats['successful_calls'] / self.call_stats['total_calls']) if self.call_stats['total_calls'] else 0
        return {
            '缓存数据量': len(self.history_cache),
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
    dynamic_base_ratio: float = 0.0

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
    streak_records: List[Dict] = field(default_factory=list)
    current_streak_type: Optional[str] = None
    current_streak_start: Optional[str] = None
    current_streak_messages: List[Dict] = field(default_factory=list)
    current_streak_count: int = 0
    last_message_id: Optional[int] = None
    prediction_content: str = "double"
    broadcast_stop_requested: bool = False
    betting_in_progress: bool = False

    def get_display_name(self) -> str:
        return self.display_name if self.display_name else self.phone

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
                    for key in ['needs_2fa', 'login_temp_data', 'chase_enabled', 'chase_numbers', 'chase_periods',
                                'chase_current', 'chase_amount', 'chase_stop_reason', 'streak_records',
                                'current_streak_type', 'current_streak_start', 'current_streak_messages',
                                'current_streak_count', 'last_message_id', 'prediction_content',
                                'broadcast_stop_requested', 'betting_in_progress']:
                        if key not in acc_dict:
                            if key == 'chase_numbers': acc_dict[key] = []
                            elif key == 'streak_records': acc_dict[key] = []
                            elif key == 'current_streak_start': acc_dict[key] = None
                            elif key == 'current_streak_messages': acc_dict[key] = []
                            elif key == 'current_streak_count': acc_dict[key] = 0
                            elif key == 'last_message_id': acc_dict[key] = None
                            elif key == 'prediction_content': acc_dict[key] = "double"
                            elif key == 'broadcast_stop_requested': acc_dict[key] = False
                            elif key == 'betting_in_progress': acc_dict[key] = False
                            elif key == 'chase_enabled': acc_dict[key] = False
                            elif key == 'chase_periods': acc_dict[key] = 0
                            elif key == 'chase_current': acc_dict[key] = 0
                            elif key == 'chase_amount': acc_dict[key] = 0
                            elif key == 'chase_stop_reason': acc_dict[key] = None
                            elif key == 'needs_2fa': acc_dict[key] = False
                            elif key == 'login_temp_data': acc_dict[key] = {}
                            elif key == 'current_streak_type': acc_dict[key] = None
                    self.accounts[phone] = Account(**acc_dict)
            except Exception as e:
                logger.log_error(0, "加载账户数据失败", e)

    async def save_accounts(self):
        data = {}
        for phone, acc in self.accounts.items():
            acc_dict = asdict(acc)
            if isinstance(acc_dict.get('current_streak_start'), datetime):
                acc_dict['current_streak_start'] = acc_dict['current_streak_start'].isoformat()
            if 'streak_records' in acc_dict:
                for record in acc_dict['streak_records']:
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
            try: await self._save_task
            except asyncio.CancelledError: pass

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
        valid_params = ['base_amount', 'max_amount', 'stop_loss', 'stop_win', 'stop_balance', 'resume_balance', 'dynamic_base_ratio']
        if param_name not in valid_params: 
            return False, f"无效参数，可选: {', '.join(valid_params)}"
        if param_name == 'base_amount' and amount > acc.balance:
            return False, f"基础金额不能超过当前余额 {acc.balance:.2f}KK"
        await self.account_manager.update_account(phone, bet_params={param_name: amount})
        logger.log_betting(user_id, "设置金额参数", f"账户:{phone} {param_name}={amount}")
        return True, f"{param_name} 已设置为 {amount}KK"

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
        await self.account_manager.update_account(
            phone,
            betting_strategy=strategy_name,
            bet_params={
                'base_amount': cfg['base_amount'], 'max_amount': cfg['max_amount'],
                'multiplier': cfg['multiplier'], 'stop_loss': cfg['stop_loss'],
                'stop_win': cfg['stop_win'], 'stop_balance': cfg.get('stop_balance', 0),
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
            'predictions': [], 'last_open_qihao': None, 'next_qihao': None,
            'last_update': None, 'cached_double_message': None, 'cached_kill_message': None
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
                    if self.last_sent_qihao.get(phone) != target_qihao:
                        msg_id = await self.send_prediction(phone, group_id, force_qihao=target_qihao)
                        if msg_id is None:
                            await self.account_manager.update_account(phone, prediction_broadcast=False, broadcast_stop_requested=False)
                            self.last_sent_qihao.pop(phone, None)
                            self._send_locks.pop(phone, None)
                            self.stop_target_qihao.pop(phone, None)
                            break
                    if self.global_predictions.get('last_open_qihao') == target_qihao:
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
        last_correct = None
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
            matched_pred['correct'] = (matched_pred['main'] == current_combo or matched_pred['candidate'] == current_combo)
            last_correct = matched_pred['correct']
            await self.model.learn(matched_pred, current_combo, current_open_qihao, current_sum)
        
        kill_combo = prediction.get('kill')
        if kill_combo is None:
            main = prediction['main']
            candidate = prediction['candidate']
            others = [c for c in COMBOS if c != main and c != candidate]
            kill_combo = random.choice(others) if others else random.choice(COMBOS)
        
        existing = None
        for i, p in enumerate(self.global_predictions['predictions']):
            if p.get('qihao') == next_qihao:
                existing = p
                break
        
        new_pred = {
            'qihao': next_qihao, 'main': prediction['main'], 'candidate': prediction['candidate'],
            'confidence': prediction['confidence'], 'time': datetime.now().isoformat(),
            'actual': None, 'sum': None, 'correct': None, 'message_id': None,
            'algo_details': prediction.get('algo_details', []), 'kill_group': kill_combo
        }
        
        if existing:
            existing.update(new_pred)
            logger.log_system(f"更新已存在的期号 {next_qihao} 的预测")
        else:
            self.global_predictions['predictions'].append(new_pred)
            if len(self.global_predictions['predictions']) > 15:
                self.global_predictions['predictions'] = self.global_predictions['predictions'][-15:]
        
        self.global_predictions['last_open_qihao'] = current_open_qihao
        self.global_predictions['next_qihao'] = next_qihao
        self.global_predictions['last_update'] = datetime.now().isoformat()
        self._update_cached_messages()
        
        tasks = []
        async def send_and_check(phone, group_id):
            acc = self.account_manager.get_account(phone)
            if not acc: 
                return
            msg_id = await self.send_prediction(phone, group_id)
            if msg_id and last_correct is not None:
                logger.log_prediction(0, f"触发连输连赢检查", f"账户:{phone} 正确:{last_correct} 消息ID:{msg_id}")
                await self._check_streak(phone, group_id, last_correct, msg_id)
        
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
        lines = ["🤖PC28双杀组+双Y融合算法 ", "-"*30, "期号    主推候选  状态  和值"]
        for p in self.global_predictions['predictions'][-15:]:
            q = p['qihao'][-4:] if len(p['qihao'])>=4 else p['qihao']
            combo_str = p['main'] + p['candidate']
            mark = "✅" if p.get('correct') is True else "❌" if p.get('correct') is False else "⏳"
            s = str(p['sum']) if p['sum'] is not None else "--"
            lines.append(f"{q:4s}   {combo_str:4s}   {mark:2s}   {s:>2s}")
        self.global_predictions['cached_double_message'] = "AI双组预测\n```" + "\n".join(lines) + "\n```"
        
        kill_lines = ["🤖AI杀组", "-"*30, "期号    杀组    状态  和值"]
        for p in self.global_predictions['predictions'][-15:]:
            q = p['qihao'][-4:] if len(p['qihao'])>=4 else p['qihao']
            kill = p.get('kill_group', '--')
            if kill is None: kill = '--'
            mark = "✅" if (p.get('actual') is not None and p['actual'] != kill) else "❌" if (p.get('actual') is not None and p['actual'] == kill) else "⏳"
            s = str(p['sum']) if p['sum'] is not None else "--"
            kill_lines.append(f"{q:4s}   {kill:4s}   {mark:2s}   {s:>2s}")
        self.global_predictions['cached_kill_message'] = "AI杀组预测\n```" + "\n".join(kill_lines) + "\n```"

    async def _check_streak(self, phone, group_id, is_correct, last_message_id):
        acc = self.account_manager.get_account(phone)
        if not acc: 
            return
        current_type = acc.current_streak_type
        current_start = acc.current_streak_start
        current_count = acc.current_streak_count
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
        
        logger.log_prediction(0, f"连输连赢更新", f"账户:{phone} 类型:{current_type} 计数:{new_count} 正确:{is_correct}")
        
        if new_count in [7,8,10]:
            message_link = f"https://t.me/c/{str(group_id).replace('-100','')}/{last_message_id}"
            record = {
                'type': current_type, 'count': new_count, 'start_time': current_start,
                'end_time': now, 'message_link': message_link, 'message_id': last_message_id, 'group_id': group_id
            }
            streak_records = acc.streak_records.copy()
            streak_records.append(record)
            if len(streak_records) > 50: 
                streak_records = streak_records[-50:]
            await self.account_manager.update_account(phone, streak_records=streak_records)
            logger.log_prediction(0, f"连输连赢记录保存", f"账户:{phone} 类型:{current_type} 计数:{new_count}")
            if new_count == 10:
                current_type = None
                current_start = None
                new_count = 0
        
        await self.account_manager.update_account(phone,
            current_streak_type=current_type,
            current_streak_start=current_start if current_start else None,
            current_streak_count=new_count
        )

    async def send_prediction(self, phone, group_id, force_qihao=None):
        lock = self._send_locks.setdefault(phone, asyncio.Lock())
        async with lock:
            target_qihao = force_qihao if force_qihao is not None else self.global_predictions.get('next_qihao')
            if self.last_sent_qihao.get(phone) == target_qihao: 
                return None
            client = self.account_manager.clients.get(phone)
            if not client or not await self.account_manager.ensure_client_connected(phone): 
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
            
            for retry in range(3):
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
                    if retry < 2: 
                        await asyncio.sleep(min(wait_seconds, 30))
                    else: 
                        logger.log_error(0, f"播报发送失败（限流）", e)
                        return None
                except Exception as e:
                    logger.log_error(0, f"发送播报失败", e)
                    return None
            return None

# ==================== 游戏调度器 ====================
class GameScheduler:
    def __init__(self, account_manager, model_manager, api_client):
        self.account_manager = account_manager
        self.model = model_manager
        self.api = api_client
        self.game_stats = {'total_cycles':0, 'betting_cycles':0, 'successful_bets':0, 'failed_bets':0, 'total_profit':0, 'total_loss':0}

    async def start_auto_betting(self, phone, user_id):
        acc = self.account_manager.get_account(phone)
        if not acc: 
            return False, "账户不存在"
        if not acc.is_logged_in: 
            return False, "请先登录账户"
        if not acc.game_group_id: 
            return False, "请先设置游戏群"
        await self.account_manager.update_account(phone, auto_betting=True, martingale_reset=True, fibonacci_reset=True)
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
            if bet_type in ["大","小"] and actual.startswith(bet_type): 
                return True
            if bet_type in ["单","双"]:
                if actual == bet_type: 
                    return True
                if len(actual)>=2 and actual[1]==bet_type: 
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
                consecutive_wins=acc.consecutive_wins+1, consecutive_losses=0,
                martingale_reset=True, fibonacci_reset=True, total_wins=acc.total_wins+1
            )
            logger.log_betting(0, "投注命中", f"账户:{phone} 期号:{expected_qihao} 实际:{actual_combo} 方案:{scheme} 主推:{main} 候选:{candidate}")
        else:
            await self.account_manager.update_account(phone, consecutive_losses=acc.consecutive_losses+1, consecutive_wins=0)
            logger.log_betting(0, "投注未命中", f"账户:{phone} 期号:{expected_qihao} 实际:{actual_combo} 方案:{scheme} 主推:{main} 候选:{candidate}")

    async def execute_chase(self, phone: str, latest: dict):
        acc = self.account_manager.get_account(phone)
        if not acc or not acc.chase_enabled: 
            return
        logger.log_betting(0, "追号检查", f"账户:{phone} 启用:{acc.chase_enabled} 进度:{acc.chase_current}/{acc.chase_periods}")
        
        if acc.chase_current >= acc.chase_periods:
            await self.account_manager.update_account(phone, chase_enabled=False, chase_stop_reason="期满",
                chase_numbers=[], chase_periods=0, chase_current=0, chase_amount=0)
            logger.log_betting(0, "追号期满停止", f"账户:{phone}")
            return
        
        current_qihao = latest.get('qihao')
        if acc.last_bet_period == current_qihao: 
            return
        
        bet_amount = acc.chase_amount if acc.chase_amount > 0 else acc.bet_params.base_amount
        bet_amount = min(bet_amount, acc.bet_params.max_amount)
        bet_amount = max(bet_amount, Config.MIN_BET_AMOUNT)
        bet_items = [f"{num}/{bet_amount}" for num in acc.chase_numbers]
        if not bet_items: 
            return
        
        total_needed = bet_amount * len(acc.chase_numbers)
        cur_bal = await self._query_balance(phone)
        if cur_bal is None or cur_bal < total_needed:
            logger.log_betting(0, "追号余额不足", f"账户:{phone} 需要:{total_needed} 余额:{cur_bal if cur_bal else '未知'}")
            return
        
        success = await self._send_bets(phone, bet_items, is_chase=True)
        if success:
            new_current = acc.chase_current + 1
            await self.account_manager.update_account(phone, chase_current=new_current, last_bet_period=current_qihao,
                last_bet_types=[str(num) for num in acc.chase_numbers], last_bet_amount=bet_amount, last_bet_total=total_needed)
            logger.log_betting(0, "追号成功", f"账户:{phone} 数字:{acc.chase_numbers} 金额:{bet_amount} 进度:{new_current}/{acc.chase_periods}")
            asyncio.create_task(self._query_balance(phone))

    async def execute_bet(self, phone, prediction, latest):
        acc = self.account_manager.get_account(phone)
        if not acc: 
            return
        lock = self.account_manager.account_locks.setdefault(phone, asyncio.Lock())
        async with lock:
            if acc.betting_in_progress:
                logger.log_betting(0, "账户投注正在进行中，跳过本次投注", f"账户:{phone}")
                return
            acc.betting_in_progress = True
        try:
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
            cur_bal = await self._query_balance(phone)
            if cur_bal is None or cur_bal <= 0: 
                return
            bet_amount, updates = self._calculate_bet_amount(acc, cur_bal)
            if updates: 
                await self.account_manager.update_account(phone, **updates)
            if acc.betting_scheme == '杀主':
                kill_combo = prediction.get('kill')
                if not kill_combo: 
                    logger.log_betting(0, "AI未提供杀组，跳过投注", f"账户:{phone}")
                    return
                bet_types = [c for c in COMBOS if c != kill_combo]
                logger.log_betting(0, f"AI杀组: {kill_combo}, 投注组合: {bet_types}", f"账户:{phone}")
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
                    last_bet_time=datetime.now().isoformat(), last_bet_amount=bet_amount, last_bet_types=bet_types,
                    total_bets=acc.total_bets+1, last_bet_total=total,
                    last_prediction={'main': prediction['main'], 'candidate': prediction['candidate'], 'confidence': prediction['confidence']},
                    last_bet_period=current_qihao)
                logger.log_betting(0, "投注成功", f"账户:{phone} 每注金额:{bet_amount} 总金额:{total} 类型:{bet_types} 置信度:{prediction['confidence']:.1f}%")
                asyncio.create_task(self._query_balance(phone))
            else: 
                self.game_stats['failed_bets'] += 1
                logger.log_betting(0, "投注失败", f"账户:{phone}")
        finally:
            async with lock: 
                acc.betting_in_progress = False

    def _calculate_bet_amount(self, acc: Account, current_balance: float) -> Tuple[int, Dict]:
        if acc.bet_params.dynamic_base_ratio > 0 and current_balance > 0:
            base = int(current_balance * acc.bet_params.dynamic_base_ratio)
            base = max(Config.MIN_BET_AMOUNT, min(base, acc.bet_params.max_amount))
        else: 
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
                fib = [1,1,2,3,5,8,13,21,34,55]
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
            return [rec[1]] if len(rec)>1 else ['小双']
        if scheme == '组合1+2': 
            return rec[:2] if len(rec)>=2 else rec
        return [rec[0]] if rec else ['小双']

    async def _send_bets(self, phone: str, bet_items: List[str], is_chase: bool) -> bool:
        client = self.account_manager.clients.get(phone)
        acc = self.account_manager.get_account(phone)
        if not client or not acc or not acc.game_group_id: 
            return False
        if not await self.account_manager.ensure_client_connected(phone): 
            return False
        message = " ".join(bet_items)
        bet_type = "追号" if is_chase else "自动投注"
        logger.log_betting(0, f"发送{bet_type}", f"账户:{phone} 消息:{message}")
        
        for retry in range(3):
            try:
                await client.send_message(acc.game_group_id, message)
                logger.log_game(f"{bet_type}发送成功: {phone} -> {message}")
                return True
            except FloodWaitError as e:
                wait_seconds = e.seconds
                logger.log_betting(0, f"触发限流，等待 {wait_seconds} 秒", f"账户:{phone}")
                if retry < 2: 
                    await asyncio.sleep(min(wait_seconds, 30))
                else:
                    logger.log_error(phone, f"{bet_type}发送失败（限流）", e)
                    try: 
                        await client.send_message(acc.game_group_id, "取消")
                    except: 
                        pass
                    return False
            except Exception as e:
                logger.log_error(phone, f"{bet_type}发送失败", e)
                try: 
                    await client.send_message(acc.game_group_id, "取消")
                except: 
                    pass
                return False
        return False

    async def _query_balance(self, phone: str) -> Optional[float]:
        client = self.account_manager.clients.get(phone)
        acc = self.account_manager.get_account(phone)
        if not client or not acc or not await self.account_manager.ensure_client_connected(phone): 
            return None
        try:
            for retry in range(3):
                try:
                    await client.send_message(Config.BALANCE_BOT, "/start")
                    break
                except FloodWaitError as e:
                    wait_seconds = e.seconds
                    logger.log_betting(0, f"余额查询触发限流，等待 {wait_seconds} 秒", f"账户:{phone}")
                    if retry < 2: 
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
                        patterns = [r'💰\s*KKCOIN\s*[:：]\s*([\d,]+\.?\d*)', r'KKCOIN\s*[:：]\s*([\d,]+\.?\d*)',
                                    r'([\d,]+\.?\d*)\s*KKCOIN', r'余额[:：]\s*([\d,]+\.?\d*)', r'([\d,]+\.?\d*)\s*元']
                        for pat in patterns:
                            m = re.search(pat, text, re.IGNORECASE)
                            if m:
                                try: 
                                    balance = float(m.group(1).replace(',',''))
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
                await self.account_manager.update_account(phone, initial_balance=balance, balance=balance,
                    last_balance=balance, last_balance_check=datetime.now().isoformat())
                return balance
            old = acc.balance
            change = balance - old
            new_profit = acc.total_profit
            new_loss = acc.total_loss
            if change > 0: 
                new_profit += change
            elif change < 0: 
                new_loss += -change
            await self.account_manager.update_account(phone, balance=balance, last_balance=old,
                last_balance_check=datetime.now().isoformat(), total_profit=new_profit, total_loss=new_loss)
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

    async def manual_bet(self, phone: str, bet_type: str, amount: int, user_id: int) -> Tuple[bool, str]:
        acc = self.account_manager.get_account(phone)
        if not acc or not acc.is_logged_in or not acc.game_group_id: 
            return False, "账户未就绪"
        valid = ['大','小','单','双','大单','大双','小单','小双']
        bet_type = bet_type.strip()
        if bet_type not in valid: 
            return False, f'无效类型，可选: {valid}'
        if amount < Config.MIN_BET_AMOUNT or amount > Config.MAX_BET_AMOUNT: 
            return False, f'金额必须在{Config.MIN_BET_AMOUNT}-{Config.MAX_BET_AMOUNT}KK之间'
        cur_bal = await self._query_balance(phone)
        if cur_bal is None: 
            return False, "余额查询失败"
        if cur_bal < amount: 
            return False, f"余额不足，当前余额: {cur_bal:.2f}KK"
        latest = await self.api.get_latest_result()
        current_qihao = latest.get('qihao') if latest else None
        success = await self._send_bets(phone, [f"{bet_type} {amount}"], is_chase=False)
        if success:
            await self.account_manager.update_account(phone,
                last_bet_time=datetime.now().isoformat(), last_bet_amount=amount, last_bet_types=[bet_type],
                total_bets=acc.total_bets+1, last_bet_period=current_qihao)
            asyncio.create_task(self._query_balance(phone))
            return True, f'已发送投注: {bet_type} {amount}KK'
        return False, '发送投注失败'

    def get_stats(self):
        auto = sum(1 for a in self.account_manager.accounts.values() if a.auto_betting)
        broadcast = sum(1 for a in self.account_manager.accounts.values() if a.prediction_broadcast)
        return {'auto_betting_accounts': auto, 'broadcast_accounts': broadcast, 'game_stats': self.game_stats.copy()}

# ==================== 全局调度器 ====================
class GlobalScheduler:
    def __init__(self, account_manager, model_manager, api_client, prediction_broadcaster, game_scheduler):
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
        self._prediction_lock = asyncio.Lock()
        self._last_prediction_result = None
        self._last_prediction_qihao = None

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
            kj_url = f"https://www.pc28.ai/api/history/kj.csv?nbr=10000"
            kj_rows = await self.api.download_csv_data(kj_url)
            logger.log_system("维护时段历史数据下载完成")
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
        for attempt in range(5):
            if await self.api.initialize_history(): 
                break
            logger.log_system(f"历史数据初始化失败，5秒后重试 ({attempt+1}/5)")
            await asyncio.sleep(5)
        
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
                if latest and latest.get('qihao') != self.last_qihao:
                    logger.log_game(f"检测到新期号: {latest['qihao']}")
                    await self._on_new_period(latest['qihao'], latest)
                
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError: 
                break
            except Exception as e: 
                logger.log_error(0, "全局调度器异常", e)
                await asyncio.sleep(10)

    async def _health_check(self):
        now = datetime.now()
        for phone, cache in list(self.account_manager.balance_cache.items()):
            if (now - cache['time']).seconds > Config.BALANCE_CACHE_SECONDS * 2:
                del self.account_manager.balance_cache[phone]
        
        if self.prediction_broadcaster.global_predictions.get('last_update'):
            last_update = datetime.fromisoformat(self.prediction_broadcaster.global_predictions['last_update'])
            if (now - last_update).total_seconds() > 86400:
                self.prediction_broadcaster.global_predictions['predictions'] = []
                self.prediction_broadcaster.global_predictions['last_open_qihao'] = None
                self.prediction_broadcaster.global_predictions['next_qihao'] = None
        
        for phone, acc in self.account_manager.accounts.items():
            if acc.is_logged_in and not await self.account_manager.ensure_client_connected(phone):
                logger.log_system(f"健康检查: 账户 {phone} 连接失效，已标记为未登录")

    async def _on_new_period(self, qihao, latest):
        try:
            for phone, acc in self.account_manager.accounts.items():
                if acc.last_bet_period and acc.last_bet_period != qihao:
                    self._create_task(self.game_scheduler.check_bet_result(phone, acc.last_bet_period, latest))
            
            history = await self.api.get_history(50)
            if len(history) < 3: 
                logger.log_game("历史数据不足，跳过预测")
                return
            
            async with self._prediction_lock:
                if self._last_prediction_qihao == qihao and self._last_prediction_result:
                    logger.log_game(f"使用缓存的预测结果: 期号 {qihao}")
                    prediction = self._last_prediction_result
                else:
                    logger.log_game(f"开始为新期号 {qihao} 生成预测（规则+AI混合）")
                    prediction = await self.model.predict(history, latest)
                    self._last_prediction_result = prediction
                    self._last_prediction_qihao = qihao
            
            next_qihao = increment_qihao(qihao)
            
            await self.prediction_broadcaster.update_global_predictions(prediction, next_qihao, latest)
            
            bet_tasks = []
            for phone, acc in self.account_manager.accounts.items():
                if (acc.auto_betting and acc.is_logged_in and acc.game_group_id and acc.last_bet_period != qihao):
                    await asyncio.sleep(random.uniform(0.1, 0.3))
                    bet_tasks.append(self._execute_bet_with_semaphore(phone, prediction, latest))
            
            if bet_tasks:
                results = await asyncio.gather(*bet_tasks, return_exceptions=True)
                for i, res in enumerate(results):
                    if isinstance(res, Exception):
                        logger.log_error(0, f"投注任务 {i} 异常", res)
            
            self.last_qihao = qihao
            
        except Exception as e:
            logger.log_error(0, f"处理新期号 {qihao} 失败", e)

    async def _execute_bet_with_semaphore(self, phone, prediction, latest):
        async with self.bet_semaphore: 
            await self.game_scheduler.execute_bet(phone, prediction, latest)

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
        logger.log_system("PC28 Bot（规则+AI混合版 - 基于文件2双杀组+双Y融合算法）初始化完成")

    def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("cancel", self.cmd_cancel))

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
            "✨ 欢迎使用！基于双杀组+双Y融合算法\n"
            "🤖 硅基流动AI辅助验证\n\n"
            "请选择操作：",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

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
            await self._show_account_detail(query, query.from_user.id, phone, context)
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
                await self._show_account_detail(query, query.from_user.id, phone, context)
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
            await self._show_account_detail(update.message, user, phone, context)
            return ConversationHandler.END
        except SessionPasswordNeededError:
            await self.account_manager.update_account(phone, needs_2fa=True)
            if hasattr(client, '_phone_code_hash'):
                acc.login_temp_data['phone_code_hash'] = client._phone_code_hash
            await update.message.reply_text("🔒 此账户启用了两步验证，请输入密码：")
            return Config.LOGIN_PASSWORD
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
            await self._show_account_detail(update.message, user, phone, context)
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
        await self._show_account_detail(query, query.from_user.id, phone, context)
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
            await self._show_account_detail(update.message, user_id, phone, context)
        else:
            await self._show_accounts_menu(update.message, user_id)

        return ConversationHandler.END

    def _get_account_detail_text_and_kb(self, phone):
        acc = self.account_manager.get_account(phone)
        if not acc:
            return "账户不存在", InlineKeyboardMarkup([])
        display = acc.get_display_name()
        status = "✅ 已登录" if acc.is_logged_in else "❌ 未登录"
        if acc.auto_betting: 
            status += " | 🤖 自动投注"
        if acc.prediction_broadcast: 
            status += " | 📊 播报中"
        if acc.broadcast_stop_requested: 
            status += " | ⏳ 停止中"

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
            [InlineKeyboardButton("💡 推荐金额", callback_data=f"recommend_amount:{phone}")],
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
            [InlineKeyboardButton("📊 连输连赢记录", callback_data=f"action:streak:{phone}"),
             InlineKeyboardButton("📚 手动投注说明", callback_data=f"action:manual_bet_help:{phone}")],
            [InlineKeyboardButton("🔙 返回", callback_data="menu:accounts")]
        ]

        if acc.chase_enabled:
            status += f" | 🔢 追{acc.chase_current}/{acc.chase_periods}"
            kb.insert(4, [InlineKeyboardButton("🛑 停止追号", callback_data=f"action:stopchase:{phone}")])

        text = f"📱 *账户: {display}*\n\n状态: {status}\n净盈利: {net_profit:.0f}K\n\n选择操作:"
        return text, InlineKeyboardMarkup(kb)

    async def _show_account_detail(self, target, user, phone, context):
        self.account_manager.set_user_state(user, 'account_selected', {'current_account': phone})
        text, reply_markup = self._get_account_detail_text_and_kb(phone)
        if hasattr(target, 'edit_message_text'):
            await target.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await target.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

    async def handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user.id
        state = self.account_manager.get_user_state(user)
        phone = state.get('current_account')
        if not phone:
            return
        acc = self.account_manager.get_account(phone)
        if not acc:
            return

        input_mode = acc.input_mode
        if input_mode and input_mode in ['base_amount', 'max_amount', 'stop_balance', 'stop_loss', 'stop_win', 'resume_balance', 'dynamic_ratio']:
            if input_mode == 'dynamic_ratio':
                try:
                    ratio = float(update.message.text.strip())
                    if ratio < 0 or ratio > 1:
                        raise ValueError
                except ValueError:
                    await update.message.reply_text("❌ 请输入0~1之间的小数，例如0.03表示3%")
                    return
                ok = await self.account_manager.update_account(phone, bet_params={'dynamic_base_ratio': ratio})
                if ok:
                    await self.account_manager.update_account(phone, input_mode=None, input_buffer='')
                    last_msg_info = context.user_data.get('last_amount_msg')
                    if last_msg_info:
                        chat_id, msg_id = last_msg_info
                        try:
                            text, reply_markup = self._get_amount_menu_text_and_kb(phone)
                            await context.bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=msg_id,
                                text=text,
                                reply_markup=reply_markup,
                                parse_mode='Markdown'
                            )
                        except Exception as e:
                            logger.log_error(user, "编辑消息失败", e)
                            await update.message.reply_text(f"✅ 动态比例已设置为 {ratio*100:.0f}%")
                        finally:
                            del context.user_data['last_amount_msg']
                    else:
                        await update.message.reply_text(f"✅ 动态比例已设置为 {ratio*100:.0f}%")
                else:
                    await update.message.reply_text("❌ 设置失败")
                return
            else:
                try:
                    amount = int(update.message.text.strip())
                except ValueError:
                    await update.message.reply_text("❌ 请输入整数金额")
                    return
                ok, msg = await self.amount_manager.set_param(phone, input_mode, amount, user)
                if ok:
                    await self.account_manager.update_account(phone, input_mode=None, input_buffer='')
                    last_msg_info = context.user_data.get('last_amount_msg')
                    if last_msg_info:
                        chat_id, msg_id = last_msg_info
                        try:
                            text, reply_markup = self._get_amount_menu_text_and_kb(phone)
                            await context.bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=msg_id,
                                text=text,
                                reply_markup=reply_markup,
                                parse_mode='Markdown'
                            )
                        except Exception as e:
                            logger.log_error(user, "编辑消息失败", e)
                            await update.message.reply_text(f"✅ {msg}")
                        finally:
                            del context.user_data['last_amount_msg']
                    else:
                        await update.message.reply_text(f"✅ {msg}")
                        await self._show_account_detail(update.message, user, phone, context)
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
            await self._show_account_detail(query, user, phone, context)
        elif data.startswith("action:"):
            parts = data.split(":")
            action = parts[1]
            phone = parts[2] if len(parts) > 2 else None
            await self._process_action(query, user, action, phone, context)
        elif data.startswith("amount_menu:"):
            phone = data.split(":")[1]
            await self._show_amount_menu_callback(query, user, phone, context)
        elif data.startswith("amount_set:"):
            parts = data.split(":")
            param_name = parts[1]
            phone = parts[2]
            await self._amount_set_callback(query, user, phone, param_name, context)
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
        elif data.startswith("recommend_amount:"):
            phone = data.split(":")[1]
            await self._show_recommend_amount_menu(query, user, phone, context)
        elif data.startswith("recommend_mode:"):
            parts = data.split(":")
            mode = parts[1]
            phone = parts[2]
            await self._calculate_and_show_recommendation(query, user, phone, mode, context)
        elif data.startswith("set_recommend:"):
            parts = data.split(":")
            answer = parts[1]
            phone = parts[2]
            amount = int(parts[3])
            if answer == 'yes':
                ok, msg = await self.amount_manager.set_param(phone, 'base_amount', amount, user)
                if ok:
                    await query.edit_message_text(f"✅ 基础金额已设置为 {amount} KK")
                else:
                    await query.edit_message_text(f"❌ 设置失败: {msg}")
            await self._show_account_detail(query, user, phone, context)
        elif data.startswith("dynamic_ratio:"):
            phone = data.split(":")[1]
            await self._show_dynamic_ratio_menu(query, user, phone, context)
        elif data.startswith("set_dynamic:"):
            parts = data.split(":")
            ratio_str = parts[1]
            phone = parts[2]
            await self._set_dynamic_ratio(query, user, phone, ratio_str, context)
        else:
            logger.log_error(user, "未知回调", data)

    def _get_amount_menu_text_and_kb(self, phone):
        acc = self.account_manager.get_account(phone)
        if not acc:
            return "账户不存在", InlineKeyboardMarkup([])
        text = f"""
💰 *金额设置*

📱 账户: {acc.get_display_name()}

当前设置:
• 基础金额: {acc.bet_params.base_amount} KK
• 最大金额: {acc.bet_params.max_amount} KK
• 停止余额: {acc.bet_params.stop_balance} KK
• 止损金额: {acc.bet_params.stop_loss} KK
• 止盈金额: {acc.bet_params.stop_win} KK
• 恢复余额: {acc.bet_params.resume_balance} KK
• 动态比例: {acc.bet_params.dynamic_base_ratio*100:.0f}%  ({'开启' if acc.bet_params.dynamic_base_ratio>0 else '关闭'})

请选择需要修改的金额类型：
        """
        kb = [
            [InlineKeyboardButton("💰 基础金额", callback_data=f"amount_set:base_amount:{phone}"),
             InlineKeyboardButton("💎 最大金额", callback_data=f"amount_set:max_amount:{phone}")],
            [InlineKeyboardButton("🛑 停止余额", callback_data=f"amount_set:stop_balance:{phone}"),
             InlineKeyboardButton("⛔ 止损金额", callback_data=f"amount_set:stop_loss:{phone}")],
            [InlineKeyboardButton("✅ 止盈金额", callback_data=f"amount_set:stop_win:{phone}"),
             InlineKeyboardButton("🔄 恢复余额", callback_data=f"amount_set:resume_balance:{phone}")],
            [InlineKeyboardButton("📈 动态投注比例", callback_data=f"dynamic_ratio:{phone}")],
            [InlineKeyboardButton("🔙 返回账户详情", callback_data=f"select_account:{phone}")]
        ]
        return text, InlineKeyboardMarkup(kb)

    async def _show_main_menu(self, query):
        kb = [
            [InlineKeyboardButton("📱 账户管理", callback_data="menu:accounts")],
            [InlineKeyboardButton("🎯 智能预测", callback_data="menu:prediction")],
            [InlineKeyboardButton("📊 系统状态", callback_data="menu:status")],
            [InlineKeyboardButton("❓ 帮助", callback_data="menu:help")],
            [InlineKeyboardButton("📖 使用手册", url=Config.MANUAL_LINK)]
        ]
        text = "🎮 *PC28 智能投注系统*\n\n基于双杀组+双Y融合算法 | AI辅助验证\n\n请选择操作："
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

    async def _show_prediction_menu(self, query):
        kb = [
            [InlineKeyboardButton("🔮 运行预测", callback_data="run_analysis")],
            [InlineKeyboardButton("🔙 返回", callback_data="menu:main")]
        ]
        await query.edit_message_text("🎯 *预测分析菜单*\n\n使用双杀组+双Y融合算法 | AI辅助验证", reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_amount_menu_callback(self, query, user, phone, context):
        context.user_data.pop('last_amount_msg', None)
        text, reply_markup = self._get_amount_menu_text_and_kb(phone)
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

    async def _amount_set_callback(self, query, user, phone, param_name, context):
        param_names = {
            'base_amount': '基础金额',
            'max_amount': '最大金额',
            'stop_balance': '停止余额',
            'stop_loss': '止损金额',
            'stop_win': '止盈金额',
            'resume_balance': '恢复余额'
        }
        display_name = param_names.get(param_name, param_name)
        self.account_manager.set_user_state(user, 'account_selected', {'current_account': phone})
        await self.account_manager.update_account(phone, input_mode=param_name, input_buffer='')
        context.user_data['last_amount_msg'] = (query.message.chat_id, query.message.message_id)
        text = f"🔢 请输入新的 {display_name}（整数KK）："
        kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"amount_menu:{phone}")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _process_action(self, query, user, action, phone, context):
        if action == "logout":
            await self._cmd_logout_inline(query, user, phone, context)
        elif action == "toggle_bet":
            acc = self.account_manager.get_account(phone)
            if acc.auto_betting:
                await self.game_scheduler.stop_auto_betting(phone, user)
            else:
                await self.game_scheduler.start_auto_betting(phone, user)
            await self._show_account_detail(query, user, phone, context)
        elif action == "toggle_pred":
            acc = self.account_manager.get_account(phone)
            if acc.prediction_broadcast:
                await self.prediction_broadcaster.stop_broadcast(phone, user)
            else:
                await self.prediction_broadcaster.start_broadcast(phone, user)
            await self._show_account_detail(query, user, phone, context)
        elif action == "balance":
            cached = self.account_manager.get_cached_balance(phone)
            if cached is not None:
                bal = cached
                text = f"💰 余额: {bal:.2f} KK (缓存)"
            else:
                bal = await self.game_scheduler._query_balance(phone)
                if bal is not None:
                    text = f"💰 余额: {bal:.2f} KK"
                else:
                    text = "❌ 查询失败"
            kb = [[InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')
        elif action == "status":
            await self._show_account_status(query, phone)
        elif action == "streak":
            await self._show_streak_records(query, phone)
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
            await self._show_account_detail(query, user, phone, context)
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
        await self._show_account_detail(query, user, phone, None)

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
        await self._show_account_detail(query, user, phone, None)

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
            await self._show_account_detail(query, user, phone, None)
        else:
            await query.edit_message_text(f"❌ {msg}", parse_mode='Markdown')

    async def _process_set_scheme(self, query, user, phone, scheme):
        ok, msg = await self.strategy_manager.set_scheme(phone, scheme, user)
        if ok:
            await self._show_account_detail(query, user, phone, None)
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
• 基础金额: {params.base_amount}KK
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

    async def _show_streak_records(self, query, phone):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return
        
        if not acc.streak_records:
            await query.edit_message_text(
                "📭 暂无连输连赢记录\n\n当有7、8、10期连输或连赢时，会自动记录。",
                parse_mode='Markdown'
            )
            return
        
        text = f"📊 *连输连赢记录 - {acc.get_display_name()}*\n\n"
        
        win_7 = [r for r in acc.streak_records if r.get('type') == 'win' and r.get('count') == 7]
        win_8 = [r for r in acc.streak_records if r.get('type') == 'win' and r.get('count') == 8]
        win_10 = [r for r in acc.streak_records if r.get('type') == 'win' and r.get('count') == 10]
        loss_7 = [r for r in acc.streak_records if r.get('type') == 'loss' and r.get('count') == 7]
        loss_8 = [r for r in acc.streak_records if r.get('type') == 'loss' and r.get('count') == 8]
        loss_10 = [r for r in acc.streak_records if r.get('type') == 'loss' and r.get('count') == 10]
        
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
            text = f"📊 *连输连赢记录 - {acc.get_display_name()}*\n\n暂无记录"
        
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
        await self.account_manager.update_account(phone, streak_records=[])
        await query.edit_message_text("✅ 所有连输连赢记录已删除")
        await self._show_account_detail(query, user, phone, None)

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

*预测算法说明*
本系统使用双杀组+双Y融合算法进行预测：
• 双杀组算法：Y值位差和 + 和值运算对立算法，双算法一致确定杀组
• 双Y融合：3Y顺序取尾数 + 双权重打分（尾数和40% + 走势频次60%）
• 硅基流动AI辅助验证：AI验证规则结果的合理性

如有问题，请联系管理员。
        """
        kb = [[InlineKeyboardButton("🔙 返回", callback_data="menu:main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _process_run_analysis(self, query):
        await query.edit_message_text("🔍 正在生成预测（使用双杀组+双Y融合算法）...")
        history = await self.api.get_history(50)
        if len(history) < 3:
            await query.edit_message_text("❌ 历史数据不足，至少需要3期数据")
            return
        latest = history[0]
        
        pred = await self.model.predict(history, latest)
        
        acc_stats = self.model.get_accuracy_stats()
        text = f"""
🎯 *PC28预测结果*

📊 *数据信息：*
• 最新期号: {latest.get('qihao', 'N/A')}
• 最新结果: {latest.get('sum', 'N/A')} ({latest.get('combo', 'N/A')})

🏆 *算法预测：*
• 主推: {pred['main']}
• 候选: {pred['candidate']}
• 杀组: {pred['kill']}
• 置信度: {pred['confidence']}%

📈 *近期准确率：{acc_stats['overall']['recent']*100:.1f}%*

🧠 *算法说明：*
• 双杀组算法（Y值位差和 + 和值运算对立）
• 双Y融合（3Y尾数顺序 + 双权重打分）
• 硅基流动AI辅助验证
        """
        kb = [[InlineKeyboardButton("🔄 刷新预测", callback_data="run_analysis")],
              [InlineKeyboardButton("🔙 返回预测菜单", callback_data="menu:prediction")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _cmd_logout_inline(self, query, user, phone, context):
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
            chase_amount=0
        )
        self.account_manager.set_user_state(user, 'idle', {'current_account': None})
        await self._show_account_detail(query, user, phone, context)

    async def _toggle_prediction_content(self, query, user, phone):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return
        new_content = "kill" if acc.prediction_content == "double" else "double"
        await self.account_manager.update_account(phone, prediction_content=new_content)
        await query.edit_message_text(f"✅ 播报内容已切换为 {'杀组' if new_content=='kill' else '双组'}")
        await self._show_account_detail(query, user, phone, None)

    async def _show_recommend_amount_menu(self, query, user, phone, context):
        kb = [
            [InlineKeyboardButton("保守", callback_data=f"recommend_mode:conservative:{phone}"),
             InlineKeyboardButton("稳定", callback_data=f"recommend_mode:stable:{phone}"),
             InlineKeyboardButton("激进", callback_data=f"recommend_mode:aggressive:{phone}")],
            [InlineKeyboardButton("🔙 返回", callback_data=f"select_account:{phone}")]
        ]
        await query.edit_message_text(
            "💡 *选择推荐模式*\n\n请选择风险偏好，系统将根据当前余额计算建议每注金额。",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode='Markdown'
        )

    async def _calculate_and_show_recommendation(self, query, user, phone, mode, context):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return
        balance = self.account_manager.get_cached_balance(phone)
        if balance is None:
            balance = await self.game_scheduler._query_balance(phone)
        if balance is None:
            await query.edit_message_text("❌ 无法获取余额，请稍后重试")
            return

        ratios = {'conservative': 0.01, 'stable': 0.02, 'aggressive': 0.05}
        recommend = int(balance * ratios.get(mode, 0.02))
        recommend = max(Config.MIN_BET_AMOUNT, min(recommend, Config.MAX_BET_AMOUNT))

        text = (
            f"💡 *推荐金额计算结果*\n\n"
            f"当前余额: {balance:.2f} KK\n"
            f"选择模式: {mode}\n"
            f"推荐每注金额: {recommend} KK\n\n"
            f"是否将此金额设置为当前账户的基础金额？"
        )
        kb = [
            [InlineKeyboardButton("✅ 是", callback_data=f"set_recommend:yes:{phone}:{recommend}"),
             InlineKeyboardButton("❌ 否", callback_data=f"select_account:{phone}")],
            [InlineKeyboardButton("🔄 重新选择", callback_data=f"recommend_amount:{phone}")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _show_dynamic_ratio_menu(self, query, user, phone, context):
        acc = self.account_manager.get_account(phone)
        if not acc:
            await query.edit_message_text("❌ 账户不存在")
            return
        current = acc.bet_params.dynamic_base_ratio
        status = f"当前动态比例：{current*100:.0f}%" if current > 0 else "当前：关闭"
        text = f"""
📈 *动态投注比例设置*

{status}

请选择投注时的动态比例（基础金额 = 余额 × 比例）：
• 关闭：使用固定的基础金额
• 1%：余额的1%（保守）
• 2%：余额的2%（稳定）
• 5%：余额的5%（激进）

选择后，每次自动投注将根据实时余额计算基础金额。
        """
        kb = [
            [InlineKeyboardButton("关闭", callback_data=f"set_dynamic:0:{phone}"),
             InlineKeyboardButton("1%", callback_data=f"set_dynamic:0.01:{phone}"),
             InlineKeyboardButton("2%", callback_data=f"set_dynamic:0.02:{phone}")],
            [InlineKeyboardButton("5%", callback_data=f"set_dynamic:0.05:{phone}"),
             InlineKeyboardButton("自定义比例", callback_data=f"set_dynamic:custom:{phone}")],
            [InlineKeyboardButton("🔙 返回金额设置", callback_data=f"amount_menu:{phone}")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    async def _set_dynamic_ratio(self, query, user, phone, ratio_str, context):
        if ratio_str == "custom":
            await self.account_manager.update_account(phone, input_mode='dynamic_ratio', input_buffer='')
            context.user_data['last_amount_msg'] = (query.message.chat_id, query.message.message_id)
            await query.edit_message_text(
                "🔢 请输入动态比例（如0.03表示3%）：\n\n范围0~1，输入0关闭动态。",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 返回", callback_data=f"dynamic_ratio:{phone}")]])
            )
            return
        try:
            ratio = float(ratio_str)
            if ratio < 0 or ratio > 1:
                raise ValueError
        except:
            await query.edit_message_text("❌ 无效比例，请重新选择")
            return
        await self.account_manager.update_account(phone, bet_params={'dynamic_base_ratio': ratio})
        await query.edit_message_text(f"✅ 动态比例已设置为 {ratio*100:.0f}%")
        await self._show_amount_menu_callback(query, user, phone, context)

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
PC28自动投注系统
基于双杀组+双Y融合算法 | AI辅助验证
部署在Railway平台
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
    bot.application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    random.seed(time.time())
    np.random.seed(int(time.time()))
    main()