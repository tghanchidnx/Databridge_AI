/**
 * DataBridge Console Dashboard
 *
 * Real-time WebSocket client for monitoring agent activity,
 * reasoning loops, and Cortex AI interactions.
 */

class ConsoleApp {
    constructor() {
        this.ws = null;
        this.connectionId = null;
        this.connected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 2000;

        // State
        this.messages = [];
        this.reasoningSteps = [];
        this.cortexMessages = [];
        this.agents = new Map();
        this.stats = {
            messages: 0,
            steps: 0,
            agents: 0,
            cortex: 0,
        };

        // Filters
        this.filters = {
            level: 'all',
            conversationId: null,
        };

        // Current tab
        this.currentTab = 'console';

        // Initialize
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.connect();
    }

    setupEventListeners() {
        // Tab switching
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', (e) => {
                this.switchTab(e.target.dataset.tab);
            });
        });

        // Level filter chips
        document.querySelectorAll('.filter-chip[data-level]').forEach(chip => {
            chip.addEventListener('click', (e) => {
                document.querySelectorAll('.filter-chip[data-level]').forEach(c => c.classList.remove('active'));
                e.target.classList.add('active');
                this.filters.level = e.target.dataset.level;
                this.renderConsole();
            });
        });

        // Conversation filter
        document.getElementById('filterConversation').addEventListener('input', (e) => {
            this.filters.conversationId = e.target.value || null;
            this.renderConsole();
        });
    }

    connect() {
        const wsUrl = `ws://${window.location.host}/ws/console`;

        try {
            this.ws = new WebSocket(wsUrl);

            this.ws.onopen = () => {
                console.log('WebSocket connected');
                this.connected = true;
                this.reconnectAttempts = 0;
                this.updateConnectionStatus(true);

                // Subscribe to all channels
                this.send({
                    type: 'subscribe',
                    payload: {
                        channels: ['console', 'reasoning', 'agents', 'cortex']
                    }
                });
            };

            this.ws.onclose = () => {
                console.log('WebSocket disconnected');
                this.connected = false;
                this.updateConnectionStatus(false);
                this.attemptReconnect();
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };

            this.ws.onmessage = (event) => {
                this.handleMessage(JSON.parse(event.data));
            };

        } catch (error) {
            console.error('Failed to connect:', error);
            this.updateConnectionStatus(false);
            this.attemptReconnect();
        }
    }

    attemptReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.log('Max reconnection attempts reached');
            return;
        }

        this.reconnectAttempts++;
        console.log(`Reconnecting in ${this.reconnectDelay}ms (attempt ${this.reconnectAttempts})`);

        setTimeout(() => {
            this.connect();
        }, this.reconnectDelay);
    }

    send(message) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(message));
        }
    }

    handleMessage(message) {
        const { type, payload, timestamp } = message;

        switch (type) {
            case 'connect':
                this.connectionId = payload.connection_id;
                this.addConsoleMessage('info', 'system', `Connected: ${payload.message}`);
                break;

            case 'subscribe':
                this.addConsoleMessage('debug', 'system', `Subscribed to: ${payload.active_subscriptions.join(', ')}`);
                break;

            case 'console.log':
                this.handleConsoleLog(payload);
                break;

            case 'reasoning.start':
            case 'reasoning.step':
            case 'reasoning.complete':
            case 'reasoning.error':
                this.handleReasoningStep(type, payload);
                break;

            case 'agent.register':
            case 'agent.status':
            case 'agent.message':
                this.handleAgentMessage(type, payload);
                break;

            case 'cortex.query':
            case 'cortex.result':
                this.handleCortexMessage(type, payload);
                break;

            case 'pong':
                // Heartbeat response
                break;

            default:
                console.log('Unknown message type:', type, payload);
        }
    }

    handleConsoleLog(payload) {
        const { level, source, message, conversation_id, timestamp } = payload;

        this.messages.push({
            timestamp: timestamp || new Date().toISOString(),
            level: level || 'info',
            source: source || 'unknown',
            message: message,
            conversationId: conversation_id,
        });

        this.stats.messages++;
        this.updateStats();

        if (this.currentTab === 'console') {
            this.renderConsole();
        }
    }

    handleReasoningStep(type, payload) {
        const step = {
            type: type.replace('reasoning.', ''),
            stepNumber: payload.step_number || this.reasoningSteps.length + 1,
            phase: payload.phase || 'execute',
            title: payload.title || 'Step',
            content: payload.content || '',
            cortexQuery: payload.cortex_query,
            cortexResult: payload.cortex_result,
            conversationId: payload.conversation_id,
            timestamp: payload.timestamp || new Date().toISOString(),
        };

        this.reasoningSteps.push(step);
        this.stats.steps++;
        this.updateStats();

        if (this.currentTab === 'reasoning') {
            this.renderReasoning();
        }

        // Also log to console
        this.addConsoleMessage('info', 'reasoning', `[${step.phase.toUpperCase()}] ${step.title}`);
    }

    handleAgentMessage(type, payload) {
        const agentId = payload.agent_id;

        if (type === 'agent.register' || type === 'agent.status') {
            this.agents.set(agentId, {
                id: agentId,
                name: payload.agent_name || agentId,
                status: payload.status || 'active',
                task: payload.current_task,
                lastActivity: new Date(),
            });
        }

        this.stats.agents = this.agents.size;
        this.updateStats();
        this.renderAgents();

        // Log to console
        if (type === 'agent.message') {
            this.addConsoleMessage('info', payload.agent_name || agentId, payload.content || payload.message);
        }
    }

    handleCortexMessage(type, payload) {
        const cortexMsg = {
            type: type.replace('cortex.', ''),
            function: payload.function,
            query: payload.query,
            result: payload.result,
            success: payload.success,
            error: payload.error,
            duration: payload.duration_ms,
            conversationId: payload.conversation_id,
            timestamp: payload.timestamp || new Date().toISOString(),
        };

        this.cortexMessages.push(cortexMsg);
        this.stats.cortex++;
        this.updateStats();

        if (this.currentTab === 'cortex') {
            this.renderCortex();
        }

        // Log to console
        const statusIcon = cortexMsg.success !== false ? '✓' : '✗';
        this.addConsoleMessage(
            cortexMsg.success !== false ? 'success' : 'error',
            'cortex',
            `${statusIcon} ${cortexMsg.function}: ${cortexMsg.query?.substring(0, 50) || 'Query'}...`
        );
    }

    addConsoleMessage(level, source, message) {
        this.messages.push({
            timestamp: new Date().toISOString(),
            level,
            source,
            message,
        });

        if (this.currentTab === 'console') {
            this.renderConsole();
        }
    }

    switchTab(tab) {
        this.currentTab = tab;

        // Update tab buttons
        document.querySelectorAll('.tab').forEach(t => {
            t.classList.toggle('active', t.dataset.tab === tab);
        });

        // Show/hide views
        document.getElementById('consoleView').style.display = tab === 'console' ? 'flex' : 'none';
        document.getElementById('reasoningView').classList.toggle('active', tab === 'reasoning');
        document.getElementById('cortexView').classList.toggle('active', tab === 'cortex');

        // Render current view
        if (tab === 'console') this.renderConsole();
        if (tab === 'reasoning') this.renderReasoning();
        if (tab === 'cortex') this.renderCortex();
    }

    renderConsole() {
        const container = document.getElementById('consoleContent');

        // Filter messages
        let filtered = this.messages;

        if (this.filters.level !== 'all') {
            filtered = filtered.filter(m => m.level === this.filters.level);
        }

        if (this.filters.conversationId) {
            filtered = filtered.filter(m =>
                m.conversationId && m.conversationId.includes(this.filters.conversationId)
            );
        }

        if (filtered.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">📋</div>
                    <div>No messages matching filters</div>
                </div>
            `;
            return;
        }

        container.innerHTML = filtered.map(m => `
            <div class="log-entry ${m.level}">
                <span class="log-time">${this.formatTime(m.timestamp)}</span>
                <span class="log-source">[${m.source}]</span>
                <span class="log-message">${this.escapeHtml(m.message)}</span>
            </div>
        `).join('');

        // Auto-scroll to bottom
        container.scrollTop = container.scrollHeight;
    }

    renderReasoning() {
        const container = document.getElementById('reasoningSteps');

        if (this.reasoningSteps.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">🧠</div>
                    <div>No reasoning steps yet</div>
                </div>
            `;
            return;
        }

        container.innerHTML = this.reasoningSteps.map(step => `
            <div class="reasoning-step">
                <div class="step-header">
                    <div class="step-number">${step.stepNumber}</div>
                    <span class="step-phase ${step.phase}">${step.phase}</span>
                    <span class="step-title">${this.escapeHtml(step.title)}</span>
                </div>
                <div class="step-content">
                    ${this.escapeHtml(step.content)}
                    ${step.cortexQuery ? `
                        <div class="step-cortex">
                            <div class="step-cortex-label">Cortex Query:</div>
                            ${this.escapeHtml(step.cortexQuery)}
                        </div>
                    ` : ''}
                    ${step.cortexResult ? `
                        <div class="step-cortex">
                            <div class="step-cortex-label">Cortex Result:</div>
                            ${this.escapeHtml(step.cortexResult)}
                        </div>
                    ` : ''}
                </div>
            </div>
        `).join('');
    }

    renderCortex() {
        const container = document.getElementById('cortexMessages');

        if (this.cortexMessages.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">❄️</div>
                    <div>No Cortex queries yet</div>
                </div>
            `;
            return;
        }

        container.innerHTML = this.cortexMessages.map(msg => `
            <div class="reasoning-step">
                <div class="step-header">
                    <span class="step-phase ${msg.success !== false ? 'execute' : 'reflect'}">${msg.function}</span>
                    <span class="step-title">${msg.type === 'query' ? 'Query' : 'Result'}</span>
                    ${msg.duration ? `<span style="color: var(--text-secondary); font-size: 0.75rem;">${msg.duration}ms</span>` : ''}
                </div>
                <div class="step-content">
                    ${msg.query ? `<div style="margin-bottom: 8px;">${this.escapeHtml(msg.query)}</div>` : ''}
                    ${msg.result ? `
                        <div class="step-cortex">
                            <div class="step-cortex-label">Result:</div>
                            ${this.escapeHtml(typeof msg.result === 'string' ? msg.result : JSON.stringify(msg.result, null, 2))}
                        </div>
                    ` : ''}
                    ${msg.error ? `<div style="color: var(--accent-red);">Error: ${this.escapeHtml(msg.error)}</div>` : ''}
                </div>
            </div>
        `).join('');
    }

    renderAgents() {
        const container = document.getElementById('agentList');

        if (this.agents.size === 0) {
            container.innerHTML = `
                <div class="empty-state" style="height: 80px;">
                    <div>No active agents</div>
                </div>
            `;
            return;
        }

        container.innerHTML = Array.from(this.agents.values()).map(agent => `
            <div class="agent-item">
                <div class="agent-avatar">${agent.name.charAt(0).toUpperCase()}</div>
                <div class="agent-info">
                    <div class="agent-name">${this.escapeHtml(agent.name)}</div>
                    <div class="agent-status ${agent.status}">${agent.status}${agent.task ? `: ${agent.task}` : ''}</div>
                </div>
            </div>
        `).join('');
    }

    updateConnectionStatus(connected) {
        const dot = document.getElementById('statusDot');
        const text = document.getElementById('statusText');

        if (connected) {
            dot.classList.add('connected');
            text.textContent = 'Connected';
        } else {
            dot.classList.remove('connected');
            text.textContent = 'Disconnected';
        }
    }

    updateStats() {
        document.getElementById('statMessages').textContent = this.stats.messages;
        document.getElementById('statSteps').textContent = this.stats.steps;
        document.getElementById('statAgents').textContent = this.stats.agents;
        document.getElementById('statCortex').textContent = this.stats.cortex;
    }

    clearConsole() {
        this.messages = [];
        this.stats.messages = 0;
        this.updateStats();
        this.renderConsole();
    }

    exportConsole() {
        const data = {
            exportedAt: new Date().toISOString(),
            messages: this.messages,
            reasoningSteps: this.reasoningSteps,
            cortexMessages: this.cortexMessages,
        };

        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);

        const a = document.createElement('a');
        a.href = url;
        a.download = `console-export-${new Date().toISOString().slice(0, 10)}.json`;
        a.click();

        URL.revokeObjectURL(url);
    }

    formatTime(isoString) {
        const date = new Date(isoString);
        return date.toLocaleTimeString('en-US', {
            hour12: false,
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Start heartbeat
    startHeartbeat() {
        setInterval(() => {
            if (this.connected) {
                this.send({ type: 'ping', payload: {} });
            }
        }, 30000);
    }
}

// Initialize app
const app = new ConsoleApp();
app.startHeartbeat();
