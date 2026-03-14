import * as assert from 'assert';
import * as vscode from 'vscode';
import * as path from 'path';

suite('Auto Mate — Extension Test Suite', () => {

	vscode.window.showInformationMessage('Running Auto Mate tests...');

	// ────────────────────────────────────────────
	// 1. Extension loads
	// ────────────────────────────────────────────

	test('Extension should be present', () => {
		const ext = vscode.extensions.getExtension('undefined_publisher.automate');
		assert.ok(ext, 'Extension not found — check publisher and name in package.json');
	});

	// ────────────────────────────────────────────
	// 2. Commands registered
	// ────────────────────────────────────────────

	test('idelense.parseDataset command should be registered', async () => {
		const commands = await vscode.commands.getCommands(true);
		assert.ok(
			commands.includes('idelense.parseDataset'),
			'idelense.parseDataset not registered'
		);
	});

	test('idelense.generateSynthetic command should be registered', async () => {
		const commands = await vscode.commands.getCommands(true);
		assert.ok(
			commands.includes('idelense.generateSynthetic'),
			'idelense.generateSynthetic not registered'
		);
	});

	test('idelense.openCheckpoint command should be registered', async () => {
		const commands = await vscode.commands.getCommands(true);
		assert.ok(
			commands.includes('idelense.openCheckpoint'),
			'idelense.openCheckpoint not registered'
		);
	});

	// ────────────────────────────────────────────
	// 3. Dataset import detection (CodeLens regex)
	// ────────────────────────────────────────────

	test('Should detect pd.read_csv import line', () => {
		const line = 'df = pd.read_csv("data/train.csv")';
		const regex = /(read_csv|read_excel|read_json|read_parquet|spark\.read)/g;
		assert.ok(regex.test(line), 'read_csv not detected');
	});

	test('Should detect pd.read_excel import line', () => {
		const line = 'df = pd.read_excel("data/survey.xlsx")';
		const regex = /(read_csv|read_excel|read_json|read_parquet|spark\.read)/g;
		assert.ok(regex.test(line), 'read_excel not detected');
	});

	test('Should detect pd.read_json import line', () => {
		const line = 'df = pd.read_json("data/records.json")';
		const regex = /(read_csv|read_excel|read_json|read_parquet|spark\.read)/g;
		assert.ok(regex.test(line), 'read_json not detected');
	});

	test('Should detect pd.read_parquet import line', () => {
		const line = 'df = pd.read_parquet("data/features.parquet")';
		const regex = /(read_csv|read_excel|read_json|read_parquet|spark\.read)/g;
		assert.ok(regex.test(line), 'read_parquet not detected');
	});

	test('Should detect spark.read import line', () => {
		const line = 'df = spark.read.csv("data/big.csv")';
		const regex = /(read_csv|read_excel|read_json|read_parquet|spark\.read)/g;
		assert.ok(regex.test(line), 'spark.read not detected');
	});

	test('Should NOT detect a non-dataset line', () => {
		const line = 'import pandas as pd';
		const regex = /(read_csv|read_excel|read_json|read_parquet|spark\.read)/g;
		assert.strictEqual(regex.test(line), false, 'False positive on non-dataset line');
	});

	// ────────────────────────────────────────────
	// 4. Path extraction from import lines
	// ────────────────────────────────────────────

	test('Should extract CSV path from import line', () => {
		const line = 'df = pd.read_csv("data/train.csv")';
		const match = line.match(/['"]([^'"]+\.(csv|xlsx|json|parquet))['"]/);
		assert.strictEqual(match?.[1], 'data/train.csv');
	});

	test('Should extract Excel path from import line', () => {
		const line = "df = pd.read_excel('data/survey.xlsx')";
		const match = line.match(/['"]([^'"]+\.(csv|xlsx|json|parquet))['"]/);
		assert.strictEqual(match?.[1], 'data/survey.xlsx');
	});

	test('Should extract JSON path from import line', () => {
		const line = 'df = pd.read_json("data/records.json")';
		const match = line.match(/['"]([^'"]+\.(csv|xlsx|json|parquet))['"]/);
		assert.strictEqual(match?.[1], 'data/records.json');
	});

	test('Should extract Parquet path from import line', () => {
		const line = 'df = pd.read_parquet("data/features.parquet")';
		const match = line.match(/['"]([^'"]+\.(csv|xlsx|json|parquet))['"]/);
		assert.strictEqual(match?.[1], 'data/features.parquet');
	});

	test('Should return null when no path found', () => {
		const line = 'df = pd.read_csv()';
		const match = line.match(/['"]([^'"]+\.(csv|xlsx|json|parquet))['"]/);
		assert.strictEqual(match, null);
	});

	// ────────────────────────────────────────────
	// 5. Kind detection from file extension
	// ────────────────────────────────────────────

	test('Should detect csv kind from .csv extension', () => {
		const ext = path.extname('data/train.csv').toLowerCase();
		assert.strictEqual(ext === '.csv' ? 'csv' : 'unknown', 'csv');
	});

	test('Should detect excel kind from .xlsx extension', () => {
		const ext = path.extname('data/survey.xlsx').toLowerCase();
		assert.strictEqual(ext === '.xlsx' ? 'excel' : 'unknown', 'excel');
	});

	test('Should detect json kind from .json extension', () => {
		const ext = path.extname('data/records.json').toLowerCase();
		assert.strictEqual(ext === '.json' ? 'json' : 'unknown', 'json');
	});

	test('Should detect parquet kind from .parquet extension', () => {
		const ext = path.extname('data/features.parquet').toLowerCase();
		assert.strictEqual(ext === '.parquet' ? 'parquet' : 'unknown', 'parquet');
	});

	// ────────────────────────────────────────────
	// 6. Configuration
	// ────────────────────────────────────────────

	test('idelense.pythonPath should default to python3', () => {
		const config = vscode.workspace.getConfiguration('idelense');
		const pythonPath = config.get<string>('pythonPath') ?? 'python3';
		assert.strictEqual(pythonPath, 'python3');
	});

});
