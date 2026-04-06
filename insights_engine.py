"""
Insights Engine — generates AI-powered marketing insights via Groq API,
plus rule-based alerts that always run regardless of API availability.
"""

import os
import logging
from typing import Optional

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


# ─── Rule-based alerts (always available) ────────────────────────────────────

def generate_rule_alerts(data: dict) -> list[dict]:
    """
    Fast, deterministic alerts based on metric thresholds.
    Returns a list of {type, level, message, entity, metric, value}.
    """
    alerts = []
    kpis   = data.get("kpis", {})
    table  = data.get("performance_table", [])

    avg_cpl  = kpis.get("overall_cpl", 0)
    avg_ctr  = kpis.get("overall_ctr", 0)
    avg_roas = kpis.get("overall_roas", 0)

    def _check_rows(rows, depth=0):
        for row in rows:
            name  = row.get("name", "Unknown")
            level = row.get("level", "unknown")
            ctr   = row.get("ctr",  0)
            cpl   = row.get("cpl",  0)
            roas  = row.get("roas", 0)
            cvr   = row.get("conversion_rate", 0)
            appt  = row.get("appointment_rate", 0)
            score = row.get("performance_score", 50)
            spend = row.get("spend", 0)

            # Low CTR
            if ctr > 0 and ctr < 1.0 and spend > 0:
                alerts.append({
                    "type":    "warning",
                    "icon":    "📉",
                    "level":   level,
                    "entity":  name,
                    "metric":  "CTR",
                    "value":   f"{ctr:.2f}%",
                    "message": f"CTR baixo ({ctr:.2f}%) em {level} '{name}' — revisar criativos ou segmentação.",
                    "action":  "Testar novos criativos",
                })

            # High CTR but low conversion (LP problem)
            if ctr > 2.0 and cvr < 3.0 and spend > 0 and cvr > 0:
                alerts.append({
                    "type":    "warning",
                    "icon":    "🔎",
                    "level":   level,
                    "entity":  name,
                    "metric":  "Conversão",
                    "value":   f"CTR {ctr:.1f}% / CVR {cvr:.1f}%",
                    "message": f"CTR alto com baixa conversão em '{name}' — possível problema na landing page.",
                    "action":  "Auditar landing page",
                })

            # CPL above average (>1.5x)
            if avg_cpl > 0 and cpl > avg_cpl * 1.5 and spend > 0:
                alerts.append({
                    "type":    "danger",
                    "icon":    "🚨",
                    "level":   level,
                    "entity":  name,
                    "metric":  "CPL",
                    "value":   f"R$ {cpl:.2f}",
                    "message": f"CPL de R$ {cpl:.2f} em '{name}' está {((cpl/avg_cpl-1)*100):.0f}% acima da média — considerar pausar.",
                    "action":  "Pausar",
                })

            # Low appointment rate from leads
            if row.get("leads", 0) > 10 and appt < 15.0 and level == "campaign":
                alerts.append({
                    "type":    "info",
                    "icon":    "📞",
                    "level":   level,
                    "entity":  name,
                    "metric":  "Taxa de Agendamento",
                    "value":   f"{appt:.1f}%",
                    "message": f"Campanha '{name}' tem muitos leads mas baixa taxa de agendamento ({appt:.1f}%) — possível gargalo no time comercial.",
                    "action":  "Revisar processo comercial",
                })

            # Top performer — scale
            if score >= 80 and spend > 0 and level == "creative":
                alerts.append({
                    "type":    "success",
                    "icon":    "🚀",
                    "level":   level,
                    "entity":  name,
                    "metric":  "Score",
                    "value":   f"{score}/100",
                    "message": f"Criativo '{name}' com score {score}/100 — candidato para escalar budget.",
                    "action":  "Escalar",
                })

            # ROAS below breakeven (assuming ~1.0 min viable)
            if roas > 0 and roas < 1.0 and spend > 0:
                alerts.append({
                    "type":    "danger",
                    "icon":    "💸",
                    "level":   level,
                    "entity":  name,
                    "metric":  "ROAS",
                    "value":   f"{roas:.2f}x",
                    "message": f"ROAS de {roas:.2f}x em '{name}' está abaixo do ponto de equilíbrio — revisar urgente.",
                    "action":  "Pausar / Revisar",
                })

            _check_rows(row.get("children", []), depth + 1)

    _check_rows(table)

    # Deduplicate by (entity, metric)
    seen = set()
    unique = []
    for a in alerts:
        key = (a["entity"], a["metric"])
        if key not in seen:
            seen.add(key)
            unique.append(a)

    return unique[:20]  # cap at 20 rule alerts


# ─── Claude AI insights ───────────────────────────────────────────────────────

def _build_prompt(data: dict) -> str:
    kpis  = data.get("kpis", {})
    table = data.get("performance_table", [])
    top5  = data.get("creative_rankings", {}).get("top_5", [])
    bot5  = data.get("creative_rankings", {}).get("bottom_5", [])

    camp_summary = []
    for c in table[:10]:  # limit to avoid huge prompts
        camp_summary.append(
            f"  • {c['name']}: gasto R${c['spend']:.0f}, "
            f"leads={c['leads']}, CPL=R${c['cpl']:.2f}, "
            f"CTR={c['ctr']:.2f}%, ROAS={c['roas']:.2f}x, "
            f"vendas={c['sales']}, score={c.get('performance_score','N/A')}"
        )

    top_names = [f"{x['name']} (score {x.get('performance_score','N/A')})" for x in top5]
    bot_names = [f"{x['name']} (CPL R${x.get('cpl',0):.2f})" for x in bot5]

    return f"""Você é um especialista em marketing de performance. Analise os dados abaixo e gere insights estratégicos em português.

RESUMO EXECUTIVO:
- Gasto total: R$ {kpis.get('total_spend', 0):.2f}
- Receita: R$ {kpis.get('total_revenue', 0):.2f}
- ROAS geral: {kpis.get('overall_roas', 0):.2f}x
- CPL médio: R$ {kpis.get('overall_cpl', 0):.2f}
- CTR médio: {kpis.get('overall_ctr', 0):.2f}%
- Total leads: {kpis.get('total_leads', 0)}
- Agendamentos: {kpis.get('total_appointments', 0)} (taxa: {kpis.get('lead_to_appt_rate', 0):.1f}%)
- Vendas: {kpis.get('total_sales', 0)} (taxa de fechamento: {kpis.get('appt_to_sale_rate', 0):.1f}%)
- CAC: R$ {kpis.get('overall_cac', 0):.2f}

CAMPANHAS:
{chr(10).join(camp_summary) if camp_summary else 'Sem dados de campanhas.'}

TOP 5 CRIATIVOS: {', '.join(top_names) if top_names else 'N/A'}
PIORES 5 CRIATIVOS: {', '.join(bot_names) if bot_names else 'N/A'}

Com base nesses dados, forneça:
1. **Diagnóstico Geral** (2-3 linhas sobre saúde geral do funil)
2. **3-5 Insights Acionáveis** (cada um com: o que está acontecendo + por que importa + ação recomendada)
3. **Prioridade Imediata** (a única coisa mais importante a fazer agora)

Seja direto, específico e orientado a decisão. Use dados concretos. Formate com markdown."""


def generate_ai_insights(data: dict) -> dict:
    """
    Calls Groq API to generate strategic insights.
    Returns {text, model, error} — never raises.
    """
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key or groq_key == "your_groq_api_key_here":
        return {
            "text":  None,
            "model": None,
            "error": "GROQ_API_KEY não configurada. Adicione sua chave do Groq no Vercel para habilitar insights com IA.",
        }

    prompt = _build_prompt(data)
    _MODELS = [
        "llama-3.3-70b-versatile",
        "llama-3.1-8b-instant",
        "mixtral-8x7b-32768",
    ]

    try:
        from groq import Groq
        client = Groq(api_key=groq_key)
    except ImportError:
        return {"text": None, "model": None, "error": "Pacote 'groq' não instalado."}

    last_err = None
    for model_name in _MODELS:
        try:
            completion = client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
                temperature=0.7,
            )
            text = completion.choices[0].message.content
            logger.info("Groq succeeded with model %s", model_name)
            return {"text": text, "model": f"groq/{model_name}", "error": None}
        except Exception as e:
            err_str = str(e)
            logger.warning("Groq model %s failed: %s", model_name, err_str)
            last_err = err_str
            continue

    logger.error("All Groq models failed. Last error: %s", last_err)
    return {"text": None, "model": None, "error": f"Erro ao gerar insights: {last_err}"}


def generate_recommendations(data: dict) -> list[dict]:
    """
    Per-creative action recommendations: Escalar / Pausar / Testar.
    Based on performance score thresholds.
    """
    recs = []
    table = data.get("performance_table", [])

    def _walk(rows):
        for row in rows:
            if row.get("level") == "creative" and row.get("spend", 0) > 0:
                score = row.get("performance_score", 50)
                name  = row.get("name", "?")
                cpl   = row.get("cpl", 0)
                roas  = row.get("roas", 0)

                if score >= 80:
                    action = "Escalar"
                    color  = "success"
                    reason = f"Score {score}/100 — performance acima da média"
                elif score <= 30:
                    action = "Pausar"
                    color  = "danger"
                    reason = f"Score {score}/100 — CPL R${cpl:.2f}, ineficiente"
                else:
                    action = "Testar Variações"
                    color  = "warning"
                    reason = f"Score {score}/100 — potencial de melhoria identificado"

                recs.append({
                    "creative":     name,
                    "campaign":     row.get("campaign_name", ""),
                    "adset":        row.get("adset_name", ""),
                    "action":       action,
                    "action_color": color,
                    "reason":       reason,
                    "score":        score,
                    "spend":        row.get("spend", 0),
                })
            _walk(row.get("children", []))

    _walk(table)
    return sorted(recs, key=lambda x: x["score"], reverse=True)[:15]
