package ServerApp2

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type CheckResult struct {
	URL     string
	Code    int
	Status  string
	Elapsed time.Duration
	Failure error
}

type Checker interface {
	Check(string) CheckResult
}

type WebChecker struct {
	httpClient *http.Client
}

func NewWebChecker(timeout time.Duration) WebChecker {
	return WebChecker{
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (w WebChecker) Check(address string) CheckResult {
	begin := time.Now()

	result := CheckResult{
		URL: address,
	}

	response, err := w.httpClient.Get(address)

	result.Elapsed = time.Since(begin)

	if err != nil {
		result.Failure = err
		return result
	}

	defer response.Body.Close()

	result.Code = response.StatusCode
	result.Status = response.Status

	return result
}

type Report struct {
	Checked int
	OK      int
	Errors  int
}

func (r *Report) update(result CheckResult) {
	r.Checked++

	if result.Failure != nil {
		r.Errors++
		return
	}

	if result.Code == http.StatusOK {
		r.OK++
	}
}

func showResult(result CheckResult) {
	if result.Failure != nil {
		fmt.Printf(
			"-> %s: Помилка (%v, %v)\n",
			result.URL,
			result.Failure,
			result.Elapsed.Round(time.Millisecond),
		)
		return
	}

	fmt.Printf(
		"-> %s: Status %d %s (%v)\n",
		result.URL,
		result.Code,
		http.StatusText(result.Code),
		result.Elapsed.Round(time.Millisecond),
	)
}

func sequential(checker Checker, addresses []string) time.Duration {
	start := time.Now()

	fmt.Println("\n[Рівень 1: послідовна перевірка]")

	for _, address := range addresses {
		showResult(checker.Check(address))
	}

	return time.Since(start)
}

func concurrentWithMutex(checker Checker, addresses []string) (Report, time.Duration) {
	start := time.Now()

	var wg sync.WaitGroup
	var lock sync.Mutex
	var report Report

	fmt.Println("\n[Рівень 2: конкурентна перевірка з Mutex]")

	for _, address := range addresses {
		wg.Add(1)

		go func(url string) {
			defer wg.Done()

			result := checker.Check(url)

			lock.Lock()
			showResult(result)
			report.update(result)
			lock.Unlock()
		}(address)
	}

	wg.Wait()

	return report, time.Since(start)
}

func concurrentWithChannel(checker Checker, addresses []string) (Report, time.Duration) {
	start := time.Now()

	results := make(chan CheckResult, len(addresses))

	var wg sync.WaitGroup

	fmt.Println("\n[Рівень 3: конкурентна перевірка через канал]")

	for _, address := range addresses {
		wg.Add(1)

		go func(url string) {
			defer wg.Done()
			results <- checker.Check(url)
		}(address)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var report Report

	for result := range results {
		showResult(result)
		report.update(result)
	}

	return report, time.Since(start)
}

func printReport(report Report, duration time.Duration) {
	fmt.Println("\n==========================================")
	fmt.Println("ПІДСУМКОВИЙ ЗВІТ")
	fmt.Println("==========================================")
	fmt.Printf("Перевірено адрес: %d\n", report.Checked)
	fmt.Printf("Відповідей 200 OK: %d\n", report.OK)
	fmt.Printf("Мережевих помилок: %d\n", report.Errors)
	fmt.Printf("Час виконання: %v\n", duration.Round(time.Millisecond))
	fmt.Println("==========================================")
}

func printPerformance(sequentialTime, concurrentTime time.Duration) {
	fmt.Println("\n==========================================")
	fmt.Println("ПОРІВНЯННЯ ШВИДКОДІЇ")
	fmt.Println("==========================================")
	fmt.Printf(
		"Послідовний режим: %v\n",
		sequentialTime.Round(time.Millisecond),
	)
	fmt.Printf(
		"Конкурентний режим: %v\n",
		concurrentTime.Round(time.Millisecond),
	)

	if concurrentTime > 0 {
		ratio := float64(sequentialTime) / float64(concurrentTime)
		fmt.Printf("Прискорення: %.2fx\n", ratio)
	}

	fmt.Println("==========================================")
}

func main() {
	fmt.Println("=== HTTP URL Health Checker ===")

	targets := []string{
		"https://go.dev",
		"https://github.com",
		"https://httpbin.org/status/404",
		"https://example.com",
		"https://invalid-fake-host-test.org",
	}

	checker := NewWebChecker(5 * time.Second)

	seqTime := sequential(checker, targets)

	fmt.Printf(
		"\nЧас послідовного виконання: %v\n",
		seqTime.Round(time.Millisecond),
	)

	mutexReport, mutexTime := concurrentWithMutex(checker, targets)

	fmt.Println()
	printReport(mutexReport, mutexTime)

	channelReport, channelTime := concurrentWithChannel(checker, targets)

	fmt.Println()
	printReport(channelReport, channelTime)

	printPerformance(seqTime, channelTime)
}
