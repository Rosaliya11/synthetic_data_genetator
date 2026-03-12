<?php
/**
 * Optional PHP endpoint to generate synthetic fraud data via Python CLI.
 * Use when you cannot run the Flask app (e.g. shared hosting with PHP only).
 * Requires: Python 3 and the generator package on the server path.
 *
 * GET/POST params: num_transactions, fraud_ratio, preset, format (csv|json)
 * Example: api.php?preset=education&format=csv
 */

header('Access-Control-Allow-Origin: *');
header('Content-Type: application/json; charset=utf-8');

$num = isset($_REQUEST['num_transactions']) ? (int) $_REQUEST['num_transactions'] : 1000;
$fraud = isset($_REQUEST['fraud_ratio']) ? (float) $_REQUEST['fraud_ratio'] : 0.02;
$preset = isset($_REQUEST['preset']) ? preg_replace('/[^a-z_]/', '', $_REQUEST['preset']) : '';
$format = isset($_REQUEST['format']) ? strtolower($_REQUEST['format']) : 'json';
if (!in_array($format, ['json', 'csv'])) $format = 'json';

$script = __DIR__ . '/run_generator.py';
if (!file_exists($script)) {
    http_response_code(500);
    echo json_encode(['error' => 'run_generator.py not found. Use Flask app.py for full API.']);
    exit;
}

$cmd = sprintf(
    'python "%s" --num %d --fraud %s --format %s',
    $script,
    max(100, min(100000, $num)),
    $fraud,
    $format
);
if ($preset !== '') $cmd .= ' --preset ' . $preset;

$output = @shell_exec($cmd);
if ($output === null) {
    http_response_code(500);
    echo json_encode(['error' => 'Python generator failed.']);
    exit;
}

if ($format === 'csv') {
    header('Content-Type: text/csv; charset=utf-8');
    header('Content-Disposition: attachment; filename="synthetic_fraud.csv"');
    echo $output;
} else {
    echo $output;
}
