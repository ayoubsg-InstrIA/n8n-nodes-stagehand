import type { BaseChatModel } from '@langchain/core/language_models/chat_models';
import type { BaseLLM } from '@langchain/core/language_models/llms';
import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { ApplicationError, assert, NodeConnectionType, NodeOperationError } from 'n8n-workflow';
import { LogLine, Stagehand as StagehandCore } from '@browserbasehq/stagehand';
import { z, ZodTypeAny } from 'zod';
import jsonToZod from 'json-to-zod';
import jsonSchemaToZod from 'json-schema-to-zod';

type Field = {
	fieldName: string;
	fieldType: string;
	optional: boolean;
};

// Filter out messages that contain image/screenshot data entirely
function sanitizeMessages(messages: LogLine[]): any[] {
	return messages
		.filter(msg => {
			const str = JSON.stringify(msg);
			return !str.includes('image') && !str.includes('screenshot') && str.length < 5000;
		})
		.map(msg => ({
			category: msg.category,
			message: msg.message,
			level: msg.level,
		}));
}

// Extract usage data from aisdk messages
function extractUsageFromMessages(messages: LogLine[]): { prompt_tokens: number; completion_tokens: number; total_tokens: number } | null {
	let totalPrompt = 0;
	let totalCompletion = 0;
	let totalTokens = 0;
	let found = false;

	for (const msg of messages) {
		if (msg.category === 'aisdk' && msg.auxiliary?.response?.value) {
			try {
				const parsed = JSON.parse(msg.auxiliary.response.value);
				if (parsed.usage) {
					found = true;
					totalPrompt += parsed.usage.prompt_tokens || parsed.usage.promptTokens || 0;
					totalCompletion += parsed.usage.completion_tokens || parsed.usage.completionTokens || 0;
					totalTokens += parsed.usage.total_tokens || parsed.usage.totalTokens || 0;
				}
			} catch {
				// Ignore parse errors
			}
		}
	}

	return found ? { prompt_tokens: totalPrompt, completion_tokens: totalCompletion, total_tokens: totalTokens } : null;
}

// Detect cache hit from messages (more reliable than token count)
function detectCacheHit(messages: LogLine[]): boolean {
	return messages.some(msg =>
		msg.category === 'cache' &&
		(msg.message === 'agent cache hit' || msg.message === 'cache hit')
	);
}

// Detect if self-heal was used (indicates LLM was called during replay)
function detectSelfHealUsed(messages: LogLine[]): boolean {
	return messages.some(msg =>
		msg.category === 'selfheal' ||
		(msg.message && msg.message.toLowerCase().includes('self-heal'))
	);
}

// Take screenshot and save to file
async function takeScreenshot(page: any, folder: string, filename: string): Promise<string> {
	const fs = await import('fs');
	const path = await import('path');

	// Ensure folder exists
	const fullFolder = path.join('/home/node', folder);
	if (!fs.existsSync(fullFolder)) {
		fs.mkdirSync(fullFolder, { recursive: true });
	}

	const filepath = path.join(fullFolder, `${filename}.png`);
	const screenshot = await page.screenshot({ type: 'png', fullPage: false });
	fs.writeFileSync(filepath, screenshot);

	return filepath;
}

export class Stagehand implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Stagehand',
		name: 'stagehand',
		icon: 'file:stagehand.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"]}}',
		description: 'Control browser using Stagehand with CDP URL',
		defaults: {
			name: 'Stagehand',
		},
		inputs: [
			NodeConnectionType.Main,
			{
				displayName: 'Model',
				maxConnections: 1,
				type: NodeConnectionType.AiLanguageModel,
				required: false,
			},
		],
		outputs: [NodeConnectionType.Main],
		usableAsTool: true,
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				options: [
					{
						name: 'Act',
						value: 'act',
						description: 'Execute an action on the page using natural language',
						action: 'Execute an action on the page',
					},
					{
						name: 'Extract',
						value: 'extract',
						description: 'Extract structured data from the page',
						action: 'Extract data from the page',
					},
					{
						name: 'Observe',
						value: 'observe',
						description: 'Observe the page and plan an action',
						action: 'Observe the page',
					},
					{
						name: 'Agent',
						value: 'agent',
						description: 'Execute a complex multi-step task autonomously',
						action: 'Run autonomous agent',
					},
				],
				default: 'act',
			},
			{
				displayName: 'CDP URL',
				name: 'cdpUrl',
				type: 'string',
				default: '',
				placeholder: 'ws://localhost:9222/devtools/browser/...',
				description: 'Chrome DevTools Protocol URL to connect to the browser',
				required: true,
			},
			{
				displayName: 'Page URL',
				name: 'pageUrl',
				type: 'string',
				default: '',
				placeholder: 'https://google.com',
				description: 'URL to navigate to before performing the action (required for act/extract)',
				required: false,
			},
			{
				displayName: 'Instructions',
				name: 'instructions',
				type: 'string',
				typeOptions: {
					rows: 4,
				},
				default: '',
				placeholder: 'Click "Accept cookies"\nType "hello" in the search box\nClick the search button',
				description: 'Instructions for Stagehand (one per line, executed in sequence)',
				required: true,
			},
			// Agent-specific options
			{
				displayName: 'Max Steps',
				name: 'maxSteps',
				type: 'number',
				default: 10,
				description: 'Maximum number of steps the agent can take to complete the task',
				displayOptions: {
					show: {
						operation: ['agent'],
					},
				},
			},
			{
				displayName: 'Agent Mode',
				name: 'agentMode',
				type: 'options',
				options: [
					{
						name: 'DOM (Recommended)',
						value: 'dom',
						description: 'Uses accessibility tree/DOM for fast, reliable element selection. This is the only stable mode.',
					},
				],
				default: 'dom',
				description: 'Agent execution mode. Currently only DOM mode is stable.',
				displayOptions: {
					show: {
						operation: ['agent'],
					},
				},
			},
			{
				displayName: 'Context',
				name: 'agentContext',
				type: 'string',
				typeOptions: {
					rows: 3,
				},
				default: '',
				placeholder: 'Additional context for the agent...',
				description: 'Additional context to help the agent understand the task',
				displayOptions: {
					show: {
						operation: ['agent'],
					},
				},
			},
			// Extract After Agent options
			{
				displayName: 'Extract After Agent',
				name: 'extractAfterAgent',
				type: 'boolean',
				default: false,
				description: 'Run an extract operation after agent completes (runs on both discovery and replay)',
				displayOptions: {
					show: {
						operation: ['agent'],
					},
				},
			},
			{
				displayName: 'Extract Instruction',
				name: 'extractInstruction',
				type: 'string',
				typeOptions: {
					rows: 2,
				},
				default: '',
				placeholder: 'Extract the birth date from the page',
				description: 'What data to extract from the page after agent completes',
				displayOptions: {
					show: {
						operation: ['agent'],
						extractAfterAgent: [true],
					},
				},
			},
			{
				displayName: 'Extract Schema Source',
				name: 'extractSchemaSource',
				type: 'options',
				options: [
					{
						name: 'Field List',
						value: 'fieldList',
					},
					{
						name: 'JSON Schema',
						value: 'jsonSchema',
					},
				],
				displayOptions: {
					show: {
						operation: ['agent'],
						extractAfterAgent: [true],
					},
				},
				default: 'jsonSchema',
			},
			{
				displayName: 'Extract Fields',
				name: 'extractFields',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
					multipleValueButtonText: 'Add Field',
					minRequiredFields: 1,
				},
				default: [],
				description: 'Fields to extract',
				options: [
					{
						displayName: 'Field',
						name: 'field',
						values: [
							{
								displayName: 'Name',
								name: 'fieldName',
								type: 'string',
								default: '',
								required: true,
							},
							{
								displayName: 'Type',
								name: 'fieldType',
								type: 'options',
								options: [
									{ name: 'String', value: 'string' },
									{ name: 'Number', value: 'number' },
									{ name: 'Boolean', value: 'boolean' },
									{ name: 'Array', value: 'array' },
									{ name: 'Object', value: 'object' },
								],
								default: 'string',
								required: true,
							},
							{
								displayName: 'Optional',
								name: 'optional',
								type: 'boolean',
								default: false,
							},
						],
					},
				],
				displayOptions: {
					show: {
						operation: ['agent'],
						extractAfterAgent: [true],
						extractSchemaSource: ['fieldList'],
					},
				},
			},
			{
				displayName: 'Extract JSON Schema',
				name: 'extractJsonSchema',
				type: 'json',
				typeOptions: {
					rows: 6,
				},
				displayOptions: {
					show: {
						operation: ['agent'],
						extractAfterAgent: [true],
						extractSchemaSource: ['jsonSchema'],
					},
				},
				default: '{\n  "type": "object",\n  "properties": {\n    "data": { "type": "string", "description": "The extracted data" }\n  },\n  "required": ["data"]\n}',
			},
			{
				displayName: 'Schema Source',
				name: 'schemaSource',
				type: 'options',
				options: [
					{
						name: 'Field List',
						value: 'fieldList',
					},
					{
						name: 'Example JSON',
						value: 'example',
					},
					{
						name: 'JSON Schema',
						value: 'jsonSchema',
					},
					{
						name: 'Manual Zod',
						value: 'manual',
					},
				],
				displayOptions: {
					show: {
						operation: ['extract'],
					},
				},
				default: 'fieldList',
				required: true,
			},
			{
				displayName: 'Fields',
				name: 'fields',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
					multipleValueButtonText: 'Add Field',
					minRequiredFields: 1,
				},
				default: [],
				description: 'List of output fields and their types',
				options: [
					{
						displayName: 'Field',
						name: 'field',
						values: [
							{
								displayName: 'Name',
								name: 'fieldName',
								type: 'string',
								default: '',
								description: 'Property name in the extracted object',
								required: true,
							},
							{
								displayName: 'Type',
								name: 'fieldType',
								type: 'options',
								options: [
									{
										name: 'Array',
										value: 'array',
									},
									{
										name: 'Boolean',
										value: 'boolean',
									},
									{
										name: 'Number',
										value: 'number',
									},
									{
										name: 'Object',
										value: 'object',
									},
									{
										name: 'String',
										value: 'string',
									},
								],
								default: 'string',
								required: true,
							},
							{
								displayName: 'Optional',
								name: 'optional',
								type: 'boolean',
								default: false,
								required: true,
							},
						],
					},
				],
				displayOptions: {
					show: {
						operation: ['extract'],
						schemaSource: ['fieldList'],
					},
				},
			},
			{
				displayName: 'Example JSON',
				name: 'exampleJson',
				type: 'json',
				typeOptions: {
					rows: 4,
				},
				displayOptions: {
					show: {
						operation: ['extract'],
						schemaSource: ['example'],
					},
				},
				default: '{\n  "title": "My Title",\n  "description": "My Description"\n}',
				required: true,
			},
			{
				displayName: 'JSON Schema',
				name: 'jsonSchema',
				type: 'json',
				typeOptions: {
					rows: 6,
				},
				displayOptions: {
					show: {
						operation: ['extract'],
						schemaSource: ['jsonSchema'],
					},
				},
				default:
					'{\n  "$schema": "http://json-schema.org/draft-07/schema#",\n  "type": "object",\n  "properties": {\n    "title": { "type": "string", "description": "The page title" },\n    "description": { "type": "string", "description": "The page description" }\n  },\n  "required": ["title", "description"]\n}',
				required: true,
			},
			{
				displayName: 'Zod Code',
				name: 'manualZod',
				type: 'string',
				typeOptions: { rows: 6 },
				displayOptions: {
					show: {
						operation: ['extract'],
						schemaSource: ['manual'],
					},
				},
				default:
					'z.object({\n  title: z.string().describe("The page title"),\n  description: z.string().describe("The page description")\n})',
				required: true,
			},
			// ADVANCED OPTIONS
			{
				displayName: 'Advanced Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				description: 'Advanced options for Stagehand',
				options: [
					{
						displayName: 'Cache Directory',
						name: 'cacheDir',
						type: 'string',
						default: '',
						placeholder: 'cache/my-workflow',
						description: 'Directory to cache actions for replay. First run uses LLM, subsequent runs replay instantly (0 tokens). Leave empty to disable caching.',
					},
					{
						displayName: 'Self Heal',
						name: 'selfHeal',
						type: 'boolean',
						default: false,
						description: 'Automatically adapt to minor DOM changes when replaying cached actions',
					},
					{
						displayName: 'DOM Settle Timeout (ms)',
						name: 'domSettleTimeoutMs',
						type: 'number',
						default: 10000,
						description: 'How long to wait for the DOM to stabilize before taking actions. Increase for slow/dynamic pages.',
					},
					{
						displayName: 'Log Messages',
						name: 'logMessages',
						type: 'boolean',
						default: false,
						description: 'Whether to include Stagehand log messages in the node output',
					},
					{
						displayName: 'Verbose Level',
						name: 'verbose',
						type: 'options',
						options: [
							{
								name: 'No Logs',
								value: 0,
							},
							{
								name: 'Only Errors',
								value: 1,
							},
							{
								name: 'All Logs',
								value: 2,
							},
						],
						default: 0,
						description: 'Level of verbosity for Stagehand internal logging',
					},
					{
						displayName: 'Take Screenshots',
						name: 'takeScreenshots',
						type: 'boolean',
						default: false,
						description: 'Take a screenshot after each action and at the end',
					},
					{
						displayName: 'Screenshots Folder',
						name: 'screenshotsFolder',
						type: 'string',
						default: 'screenshots',
						placeholder: 'screenshots',
						description: 'Folder to save screenshots (relative to /home/node/)',
						displayOptions: {
							show: {
								takeScreenshots: [true],
							},
						},
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const results: INodeExecutionData[] = [];
		const model = await this.getInputConnectionData(NodeConnectionType.AiLanguageModel, 0);

		assert(Stagehand.isChatInstance(model), 'A Chat Model is required');
		assert('model' in model, 'Model is not defined in the input connection data');
		assert('apiKey' in model, 'API Key is not defined in the input connection data');
		assert(typeof model.model === 'string', 'Model must be a string');
		assert(typeof model.apiKey === 'string', 'API Key must be a string');

		for (let i = 0; i < items.length; i++) {
			const operation = this.getNodeParameter('operation', i) as string;
			const cdpUrl = this.getNodeParameter('cdpUrl', i, '') as string;

			// Advanced options
			const cacheDir = this.getNodeParameter('options.cacheDir', i, '') as string;
			const selfHeal = this.getNodeParameter('options.selfHeal', i, false) as boolean;
			const domSettleTimeoutMs = this.getNodeParameter('options.domSettleTimeoutMs', i, 10000) as number;
			const logMessages = this.getNodeParameter('options.logMessages', i, false) as boolean;
			const verbose = this.getNodeParameter('options.verbose', i, 0) as 0 | 1 | 2;
			const takeScreenshots = this.getNodeParameter('options.takeScreenshots', i, false) as boolean;
			const screenshotsFolder = this.getNodeParameter('options.screenshotsFolder', i, 'screenshots') as string;

			// Always capture messages for usage tracking (even if logMessages is false)
			const messages: LogLine[] = [];
			// Map provider names to what Stagehand expects
			let provider = model.lc_namespace[2];
			if (provider === 'google_genai' || provider === 'google_vertexai') {
				provider = 'google';
			} else if (model.model.includes('deepseek')) {
				provider = 'deepseek';
			}

			// Debug logging
			console.log('[Stagehand Debug] Provider:', provider);
			console.log('[Stagehand Debug] Model:', model.model);
			console.log('[Stagehand Debug] Full modelName:', provider + '/' + model.model);
			console.log('[Stagehand Debug] CDP URL:', cdpUrl);
			if (cacheDir) {
				console.log('[Stagehand Debug] Cache Dir:', cacheDir, '(replay mode enabled)');
			}

			const stagehand = new StagehandCore({
				env: 'LOCAL',
				experimental: true,
				verbose,
				selfHeal,
				domSettleTimeout: domSettleTimeoutMs,
				// Enable action caching/replay if cacheDir is specified
				...(cacheDir ? { cacheDir } : {}),
				// Always capture messages for usage tracking
				// (logMessages option controls whether they appear in output)
				logger: (message) => {
					messages.push(message);
				},
				// V3 API: model as ModelConfiguration object with modelName + apiKey
				model: {
					modelName: provider + '/' + model.model,
					apiKey: model.apiKey,
				} as any,
				localBrowserLaunchOptions: {
					cdpUrl,
				},
			});
			await stagehand.init();

			// V3 API: get the page from context
			const pages = stagehand.context.pages();
			const page = pages[0];

			// Navigate to page URL if provided
			const pageUrl = this.getNodeParameter('pageUrl', i, '') as string;
			if (pageUrl) {
				console.log('[Stagehand Debug] Navigating to:', pageUrl);
				await page.goto(pageUrl, { waitUntil: 'domcontentloaded' });
				console.log('[Stagehand Debug] Navigation complete');
			}

			try {
				switch (operation) {
					case 'act': {
						const instructionsRaw = this.getNodeParameter('instructions', i, '') as string;
						const instructions = instructionsRaw.split('\n').map(s => s.trim()).filter(s => s.length > 0);

						const actResults: any[] = [];
						for (const instruction of instructions) {
							console.log('[Stagehand Debug] Executing instruction:', instruction);
							// V3 API: act() is on stagehand, not page
							const result = await stagehand.act(instruction);
							actResults.push({ instruction, result });
						}

						// Extract usage to check for cache hit
						const actUsage = extractUsageFromMessages(messages);
						const isCacheHit = actUsage && actUsage.total_tokens === 0;

						results.push({
							json: {
								operation,
								results: actResults,
								cacheHit: isCacheHit,
								...(cacheDir ? { cacheDir } : {}),
								usage: actUsage,
								currentUrl: page.url(),
								...(logMessages ? { messages: sanitizeMessages(messages) } : {}),
							},
						});
						break;
					}

					case 'extract': {
						const instructionsRaw = this.getNodeParameter('instructions', i, '') as string;
						const instruction = instructionsRaw.split('\n')[0]?.trim() || '';
						const schemaSource = this.getNodeParameter('schemaSource', i, 'example') as string;

						let schema: z.ZodObject<any>;
						switch (schemaSource) {
							case 'fieldList': {
								const fields = this.getNodeParameter('fields.field', i, []) as any[];
								schema = Stagehand.fieldsToZodSchema(fields);
								break;
							}

							case 'example': {
								const example = this.getNodeParameter('exampleJson', i) as string;
								schema = new Function('z', `${jsonToZod(JSON.parse(example))}return schema;`)(z);
								break;
							}

							case 'jsonSchema': {
								const jsonSchema = this.getNodeParameter('jsonSchema', i) as string;
								schema = new Function('z', `return ${jsonSchemaToZod(JSON.parse(jsonSchema))};`)(z);
								break;
							}

							case 'manual': {
								const zodCode = this.getNodeParameter('manualZod', i) as string;
								schema = new Function('z', `return ${zodCode};`)(z);
								break;
							}

							default: {
								throw new ApplicationError(`Unsupported schema source: ${schemaSource}`);
							}
						}

						// V3 API: extract() is on stagehand, not page
						// Cast to any to avoid TypeScript deep instantiation error
						const extractResult = await (stagehand.extract as any)(instruction, schema);
						results.push({
							json: {
								operation,
								result: extractResult,
								...(logMessages ? { messages: sanitizeMessages(messages) } : {}),
							},
						});
						break;
					}

					case 'observe': {
						const instructionsRaw = this.getNodeParameter('instructions', i, '') as string;
						const instruction = instructionsRaw.split('\n')[0]?.trim() || '';

						// V3 API: observe() is on stagehand, not page
						results.push({
							json: {
								operation,
								result: await stagehand.observe(instruction),
								...(logMessages ? { messages: sanitizeMessages(messages) } : {}),
							},
						});
						break;
					}

					case 'agent': {
						const instructionsRaw = this.getNodeParameter('instructions', i, '') as string;
						const instruction = instructionsRaw.trim();
						const maxSteps = this.getNodeParameter('maxSteps', i, 10) as number;
						const agentMode = this.getNodeParameter('agentMode', i, 'dom') as 'dom';
						const agentContext = this.getNodeParameter('agentContext', i, '') as string;

						// Extract After Agent options
						const extractAfterAgent = this.getNodeParameter('extractAfterAgent', i, false) as boolean;

						console.log('[Stagehand Debug] Agent mode:', agentMode);
						console.log('[Stagehand Debug] Max steps:', maxSteps);

						// Parse extract schema BEFORE agent execution so we can pass it
						// as output schema to the agent (used in discovery mode)
						let extractSchema: z.ZodObject<any> | null = null;
						if (extractAfterAgent) {
							console.log('[Stagehand Debug] Extract After Agent: enabled');
							const extractSchemaSource = this.getNodeParameter('extractSchemaSource', i, 'jsonSchema') as string;
							if (extractSchemaSource === 'fieldList') {
								const fields = this.getNodeParameter('extractFields.field', i, []) as any[];
								extractSchema = Stagehand.fieldsToZodSchema(fields);
							} else {
								const jsonSchema = this.getNodeParameter('extractJsonSchema', i, '{}') as string;
								extractSchema = new Function('z', `return ${jsonSchemaToZod(JSON.parse(jsonSchema))};`)(z);
							}
						}

						// Create agent with mode and execute
						// agentContext goes as systemPrompt on agent(), NOT context on execute()
						const agent = stagehand.agent({
							mode: agentMode,
							...(agentContext ? { systemPrompt: agentContext } : {}),
						});
						const agentResult = await agent.execute({
							instruction,
							maxSteps,
						});

						// Take final screenshot if enabled
						const screenshotPaths: string[] = [];
						if (takeScreenshots) {
							const timestamp = Date.now();
							const screenshotPath = await takeScreenshot(page, screenshotsFolder, `agent-final-${timestamp}`);
							screenshotPaths.push(screenshotPath);
							console.log('[Stagehand Debug] Screenshot saved:', screenshotPath);
						}

						// Extract usage from aisdk messages (contains token counts)
						const usage = extractUsageFromMessages(messages);

						// Detect cache hit from messages (more reliable)
						const isCacheHit = detectCacheHit(messages);
						const selfHealUsed = detectSelfHealUsed(messages);

						// Cache-aware extraction:
						// - Discovery (no cache): agent() internally does act/observe/extract
						//   as part of its autonomous work. Post-extract is skipped.
						//   extractResult comes from the agent's own output.
						// - Replay (cache hit): act() steps replayed from cache (0 tokens),
						//   then post-extract runs fresh with LLM for structured data.
						let extractResult: any = null;
						let extractUsage: any = null;
						if (extractAfterAgent && isCacheHit) {
							// Replay: run post-extract with LLM
							console.log('[Stagehand Debug] Replay mode - running post-extract');
							const extractInstr = this.getNodeParameter('extractInstruction', i, '') as string;

							const messagesBeforeExtract = messages.length;
							extractResult = await (stagehand.extract as any)(extractInstr, extractSchema);
							const extractMessages = messages.slice(messagesBeforeExtract);
							extractUsage = extractUsageFromMessages(extractMessages);
							console.log('[Stagehand Debug] Post-extract completed');
						} else if (extractAfterAgent && !isCacheHit) {
							// Discovery: agent handled extraction internally.
							// Find the extract action in the agent's actions array to get the actual extracted data.
							console.log('[Stagehand Debug] Discovery mode - looking for extract action in agent results');
							console.log('[Stagehand Debug] All actions:', JSON.stringify(agentResult.actions, null, 2));
							const extractAction = agentResult.actions.find((a: any) => a.type === 'extract');
							if (extractAction) {
								console.log('[Stagehand Debug] Found extract action:', JSON.stringify(extractAction, null, 2));
								// Try different possible locations for extract data
								extractResult = extractAction.result || extractAction.data || extractAction.output || extractAction;
							} else {
								// Fallback to agent's message if no extract action found
								console.log('[Stagehand Debug] No extract action found, using agent message');
								extractResult = { data: agentResult.message };
							}
							console.log('[Stagehand Debug] Agent extractResult:', JSON.stringify(extractResult));
						}

						// Simplify actions for cleaner output
						const simplifiedActions = agentResult.actions.map((action: any) => ({
							type: action.type,
							reasoning: action.reasoning,
							parameters: action.parameters,
							taskCompleted: action.taskCompleted,
						}));

						// Workaround: Vercel AI SDK bug causes ModelMessage[] error on 'done' tool
						// even when extraction succeeded. If we have extractResult, consider it successful.
						const effectiveSuccess = agentResult.success || (extractAfterAgent && extractResult != null);

						results.push({
							json: {
								operation,
								success: effectiveSuccess,
								message: agentResult.message,
								completed: agentResult.completed || effectiveSuccess,
								actions: simplifiedActions,
								actionCount: agentResult.actions.length,
								mode: agentMode,
								cacheHit: isCacheHit,
								selfHealUsed,
								...(extractResult ? { extractResult, extractUsage } : {}),
								...(cacheDir ? { cacheDir } : {}),
								usage,
								currentUrl: page.url(),
								...(screenshotPaths.length > 0 ? { screenshots: screenshotPaths } : {}),
								...(logMessages ? { messages: sanitizeMessages(messages) } : {}),
							},
						});
						break;
					}

					default: {
						throw new ApplicationError(`Unsupported operation: ${operation}`);
					}
				}
			} catch (error) {
				results.push({
					error: new NodeOperationError(this.getNode(), error as Error, {
						message: `Error executing Stagehand operation: ${error.message}`,
					}),
					json: {
						operation,
						...(logMessages ? { messages: sanitizeMessages(messages) } : {}),
					},
				});
			} finally {
				await stagehand.close();
			}
		}

		return [results];
	}

	static isChatInstance(model: unknown): model is BaseChatModel {
		const namespace = (model as BaseLLM)?.lc_namespace ?? [];

		return namespace.includes('chat_models');
	}

	static fieldsToZodSchema(fields: Field[]): z.ZodObject<any> {
		const shape: Record<string, ZodTypeAny> = {};

		for (const { fieldName, fieldType, optional } of fields) {
			let zType: ZodTypeAny;

			switch (fieldType) {
				case 'string':
					zType = z.string();
					break;
				case 'number':
					zType = z.number();
					break;
				case 'boolean':
					zType = z.boolean();
					break;
				case 'array':
					zType = z.array(z.any());
					break; // puoi espandere
				case 'object':
					zType = z.object({}).passthrough();
					break;
				default:
					zType = z.any();
			}

			shape[fieldName] = optional ? zType.optional() : zType;
		}

		return z.object(shape);
	}
}
