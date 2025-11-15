# llm_server.py
# Flask LLM helper with optional Gemini (Google) integration + safe local fallback.

from flask import Flask, request, jsonify
import json, re, os, time
from pathlib import Path

# optional import - may raise if not installed; we'll handle gracefully
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except Exception:
    GEMINI_AVAILABLE = False

app = Flask(__name__)

# Local booking file (same as before)
DATA_FILE = Path("node_node1.json")

# -----------------------
# Local helper (safe)
# -----------------------
def get_seat_status_local(seat_id: str) -> dict:
    seat_id = (seat_id or "").upper().strip()
    if not seat_id:
        return {"error": "No seat id provided."}
    try:
        if not DATA_FILE.exists():
            return {"error": "Booking data file not found on this node."}
        data = json.loads(DATA_FILE.read_text())
        seat_data = data.get("seats", {})
        if seat_id not in seat_data:
            return {"error": f"Seat {seat_id} not valid."}
        info = seat_data[seat_id]
        if info.get("reserved"):
            return {"seat_id": seat_id, "status": "booked", "reserved_by": info.get("by", "unknown")}
        else:
            return {"seat_id": seat_id, "status": "available"}
    except Exception as exc:
        return {"error": f"Could not read booking data ({exc})."}

# -----------------------
# Gemini wrapper (optional)
# -----------------------
def ask_gemini_with_tool(query: str, timeout_seconds: int = 10) -> str:
    """
    Ask Gemini with a tool (get_seat_status). The function returns Gemini's final text answer.
    If anything goes wrong or GEMINI is not configured, raises Exception.
    """
    if not GEMINI_AVAILABLE:
        raise RuntimeError("google-genai library not installed")

    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    client = genai.Client(api_key) if hasattr(genai, "Client") else genai.Client()

    # Define the Python tool we expose. The model will call it via function-calling mechanism.
    # We expose get_seat_status which reads local file (the server process will execute it).
    def get_seat_status_tool(seat_id: str) -> str:
        # return JSON string (tool output) — model can parse it
        result = get_seat_status_local(seat_id)
        return json.dumps(result)

    # Configure model to use the tool
    config = types.GenerateContentConfig(
        tools=[get_seat_status_tool]
    )

    # first generation: let model decide whether to call tool
    resp = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=query,
        config=config
    )

    # If model requested tool call(s) -> execute them & give result back
    if resp and getattr(resp, "function_calls", None):
        # support single function call flow (simple)
        fc = resp.function_calls[0]
        fname = fc.name
        args = dict(fc.args) if getattr(fc, "args", None) else {}
        # only one tool exposed => get_seat_status_tool
        if fname == "get_seat_status":
            seat_arg = args.get("seat_id") or args.get("seat") or ""
            tool_output = get_seat_status_tool(seat_arg)
            # send back the tool output for final answer
            second_resp = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    types.Content(role="user", parts=[types.Part.from_text(query)]),
                    resp.candidates[0].content,
                    types.Content(
                        role="tool",
                        parts=[types.Part.from_function_response(name=fname, response={"result": tool_output})]
                    )
                ],
                timeout=timeout_seconds
            )
            # return the final model text
            return getattr(second_resp, "text", None) or (second_resp.candidates[0].content.text if getattr(second_resp, "candidates", None) else None)

    # else: no function call, return model text
    return getattr(resp, "text", None) or (resp.candidates[0].content.text if getattr(resp, "candidates", None) else None)

# -----------------------
# Flask endpoint: /ask
# -----------------------
@app.route("/ask", methods=["POST"])
def handle_query():
    payload = request.get_json() or {}
    query = (payload.get("q") or "").strip()
    # quick sanitize
    if not query:
        return jsonify({"answer": "Please ask something about seats, e.g., 'is S1 available?'"})

    # quick seat detection (so we can short-circuit)
    seat_match = re.search(r"\b(s\d{1,2})\b", query, re.IGNORECASE)
    seat_id = seat_match.group(1).upper() if seat_match else None

    # 1) If seat explicitly asked, use local file directly (fast and deterministic).
    if seat_id:
        local = get_seat_status_local(seat_id)
        # build a friendly string response
        if "error" in local:
            fallback_msg = local["error"]
        else:
            if local.get("status") == "booked":
                fallback_msg = f"Seat {local['seat_id']} is booked by {local.get('reserved_by','unknown')}."
            else:
                fallback_msg = f"Seat {local['seat_id']} is available."
        # Optionally: still call Gemini for a richer answer if configured
        if GEMINI_AVAILABLE and os.getenv("GEMINI_API_KEY"):
            try:
                gem_resp = ask_gemini_with_tool(query)
                if gem_resp and gem_resp.strip():
                    return jsonify({"answer": gem_resp})
            except Exception as e:
                # fall back to local answer
                app.logger.warning("Gemini call failed, using local answer: %s", e)
        return jsonify({"answer": fallback_msg})

    # 2) If no explicit seat id, if Gemini available, ask Gemini for a natural answer
    if GEMINI_AVAILABLE and os.getenv("GEMINI_API_KEY"):
        try:
            gem_resp = ask_gemini_with_tool(query)
            if gem_resp:
                return jsonify({"answer": gem_resp})
        except Exception as e:
            app.logger.warning("Gemini error: %s", e)

    # 3) final fallback: generic reply based on local data
    try:
        # try to use local knowledge to give something useful
        if DATA_FILE.exists():
            data = json.loads(DATA_FILE.read_text())
            seats = data.get("seats", {})
            free = [k for k,v in seats.items() if not v.get("reserved")]
            sample = f"{len(free)} seats available. Example free seats: {', '.join(free[:3])}." if free else "No seats currently free."
            return jsonify({"answer": f"I'm not sure, but here is local info: {sample}"})
    except Exception:
        pass

    return jsonify({"answer": "I'm not sure about that."})

# -----------------------
# Run server
# -----------------------
if __name__ == "__main__":
    print("LLM service running on port 8000 (Gemini available: {})".format(GEMINI_AVAILABLE and bool(os.getenv("GEMINI_API_KEY"))))
    app.run(host="0.0.0.0", port=8000)
