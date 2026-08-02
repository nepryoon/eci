package main

// Notifier è un nodo Interface nel CPG.
type Notifier interface {
	Notify(message string) error
}

// EmailNotifier è un nodo Class (struct) che implementa Notifier.
type EmailNotifier struct {
	FromAddress string
}

// Notify è un nodo Method (receiver *EmailNotifier), nessuna chiamata in
// uscita — questo file esercita il nodo Interface e la sua implementazione,
// non relazioni CALLS.
func (e *EmailNotifier) Notify(message string) error {
	return nil
}
