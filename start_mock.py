#!/usr/bin/env python3
import subprocess
import time
import sys
import os
import signal

def main():
    print("Starting NexGen mock environment on your laptop...")
    
    # Environment variables for mock mode
    env = os.environ.copy()
    env["MOCK_SERVICES"] = "true"
    
    # Start the backend services in the background
    procs = []
    try:
        print("[1/4] Starting Master Orchestrator (Port 8000)...")
        master_proc = subprocess.Popen(
            ["uv", "run", "--project", "master", "uvicorn", "src.main:app", "--port", "8000"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        procs.append(master_proc)
        
        print("[2/4] Starting Query Service (Port 8001)...")
        query_proc = subprocess.Popen(
            ["uv", "run", "--project", "query", "uvicorn", "src.main:app", "--port", "8001"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        procs.append(query_proc)
        
        print("[3/4] Starting RAG Service (Port 8002)...")
        rag_proc = subprocess.Popen(
            ["uv", "run", "--project", "rag", "uvicorn", "src.main:app", "--port", "8002"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        procs.append(rag_proc)
        
        print("Waiting 5 seconds for backend APIs to boot...")
        time.sleep(5)
        
        print("[4/4] Launching the Terminal UI (TUI)...")
        # Run TUI in the foreground
        subprocess.run(["uv", "run", "--project", "tui", "python", "-m", "tui"], env=env)
        
    except KeyboardInterrupt:
        print("\nShutting down NexGen environment...")
    finally:
        # Cleanup child processes
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            p.wait()
        print("All backend services stopped.")

if __name__ == "__main__":
    main()
