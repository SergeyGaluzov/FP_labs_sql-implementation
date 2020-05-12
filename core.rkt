#lang racket
(require 2htdp/batch-io)
(require racket/set)

(define (transpose lst)
  (if (= 0 (length (first lst))) '() (append (list (map (lambda (elem) (first elem)) lst)) (transpose (map (lambda (x) (remove (first x) x)) lst))))
)

(define (append-at lst item position) (append (take lst position) (cons item (drop lst position))) )

(define (lists file)  
  (if (string-contains? file ".csv") (read-csv-file  (string-append "tables/" file))
    (map (lambda (x) (string-split x #px"\t")) (read-lines (string-append "tables/" file))))
)
(define (slice list start end) (take (drop list start) (- end start)) )

(define (select lst selected_columns group_columns) 
  (define (create_cols_list all_columns columns)
    (
      let
      (
        [columns_without_cases (filter (lambda (x) (string? x)) columns)]
      )
      (if (null? columns_without_cases) '() 
      (if (equal? (first columns_without_cases) "*") 
        (append all_columns (create_cols_list all_columns (rest columns_without_cases))) 
        (append (list (first columns_without_cases)) (create_cols_list all_columns (rest columns_without_cases))))
      )
    )
  )
  (define (apply_function elem lst)
    (define (max x1 x2) (if (> x1 x2) x1 x2))
    (let*
      (
        [param (substring elem (add1 (index-of (string->list elem) #\( )) (index-of (string->list elem) #\) ) )]
        [function (substring elem 0 (index-of (string->list elem) #\( ))]
      )
      (let* 
        ([g (if (equal? param "*") (rest lst) (map (lambda (x) (string->number (first (filter (lambda (y) (equal? y (list-ref x (index-of (first lst) param)))) x))) ) (rest lst)))])
        (cond 
          [(equal? function "count") (number->string (length g))]
          [(equal? function "avg") (number->string (exact->inexact (/ (foldl + 0 g) (length g))))]
          [(equal? function "max") (number->string (foldl max (first g) (rest g)))]
        )
      )
    )
  )
  (define (only_columns lst indexes_of_selected_cols)
    (if (null? lst) '()
      (append 
      (list (map (lambda (x) (list-ref (first lst) x)) indexes_of_selected_cols)) 
      (only_columns (rest lst) indexes_of_selected_cols))
    )
  )
  (define (only_aggregate_functions sel_cols lst)
   (append (list sel_cols) (list (map (lambda (x) (apply_function x lst)) sel_cols)))
  )

  (define (create_index_list all_columns cols) (map (lambda (x) (index-of all_columns x)) cols))

  (define (group_by selected_cols_list lst group_columns)
    (define (create_groups_list lst group_records group_index_list)
      (if (null? group_records) '() 
        (let*
          (
            [appropriate_lists_foreach_group_record 
              (filter (lambda (lst_row) (equal? (first group_records) (map (lambda (index) (list-ref lst_row index)) group_index_list))) lst)
            ]
          )
          (append 
            (list appropriate_lists_foreach_group_record)
            (create_groups_list (remove* appropriate_lists_foreach_group_record lst) (rest group_records) group_index_list)
          )
        )
      )
    )

    (
      let*
      (
        [group_list (create_cols_list (first lst) group_columns)]
        [group_index_list (create_index_list (first lst) group_list)]
        [group_records (remove-duplicates (only_columns lst group_index_list))] 
        [groups_list (create_groups_list (rest lst) (rest group_records) group_index_list)]
      )
      (transpose 
        (map 
          (lambda (elem) 
            (if (member elem (first group_records)) 
              (map (lambda (group_record) (list-ref group_record (index-of (first group_records) elem))) group_records)
              (append (list elem) (map (lambda (group) (apply_function elem (append (list (first lst)) group))) groups_list))
            )
          )
          selected_cols_list
        )
      )
    )
  )
  
  (let*
    (
      [all_columns (first lst)]
      [selected_cols_list (create_cols_list all_columns selected_columns)]
    )
    (if group_columns
      (group_by selected_cols_list lst group_columns)
      (if (null? (filter (lambda (x) (member x all_columns)) selected_cols_list))
        (only_aggregate_functions selected_cols_list lst)
        (
          let 
          (
            [indexes_of_selected_cols (create_index_list all_columns selected_cols_list)]
          )
          (only_columns lst indexes_of_selected_cols) 
        )
      )
    )
  )
)

(define (distinct is_distinct selected_cols) (if is_distinct (remove-duplicates selected_cols) selected_cols))

(define (split_condition_by_and/or/not conditions)
      (
        let*
        (
          [and_index (index-of conditions "and") ]
          [or_index (index-of conditions "or")]
          [not_index (index-of conditions "not")]
          [seperator_position (min (if and_index and_index (length conditions)) (if or_index or_index (length conditions)) (if not_index not_index (length conditions)))] 
        )

        (if (null? conditions) '() 
        
        (if (= 0 seperator_position) (append (list (first conditions)) (split_condition_by_and/or/not (rest conditions)))    
        (append (list (slice conditions 0 seperator_position ) ) (split_condition_by_and/or/not (list-tail conditions seperator_position))))
        )
      )
)

(define (rows_after_applying_conditions init_list conditions cond_list)
  (if (null? conditions) cond_list
    (let*
    (
      [cols_names (first init_list)]
    )
    (if (not conditions) (rest init_list)
      (if (list? (first conditions))
      (
        let*
        (
          [condition (first conditions)]
          [col_name (first condition)]
          [index_col_name (index-of cols_names col_name)]
          [cond_sign (second condition)]
          [value (if (> (length (cddr condition)) 1) (string-join (cddr condition)) (third condition))]
        )
        (cond
          [(equal? cond_sign "<=") (rows_after_applying_conditions init_list (rest conditions) (filter (lambda (x) (and (> (index-of init_list x) 0) (<= (string->number (list-ref x index_col_name)) (string->number value)))) cond_list))]
          [(equal? cond_sign "<>") (rows_after_applying_conditions init_list (rest conditions) (filter (lambda (x) (and (> (index-of init_list x) 0) (not (equal? (list-ref x index_col_name) value)))) cond_list))]
          [(equal? cond_sign "<") (rows_after_applying_conditions init_list (rest conditions) (filter (lambda (x) (and (> (index-of init_list x) 0) (< (string->number (list-ref x index_col_name)) (string->number value)))) cond_list))]
          [(equal? cond_sign ">=") (rows_after_applying_conditions init_list (rest conditions) (filter (lambda (x) (and (> (index-of init_list x) 0) (>= (string->number (list-ref x index_col_name)) (string->number value)))) cond_list))]
          [(equal? cond_sign ">") (rows_after_applying_conditions init_list (rest conditions) (filter (lambda (x) (and (> (index-of init_list x) 0) (> (string->number (list-ref x index_col_name)) (string->number value)))) cond_list))]
          [(equal? cond_sign "=") (rows_after_applying_conditions init_list (rest conditions) (filter (lambda (x) (and (> (index-of init_list x) 0) (equal? (list-ref x index_col_name) value))) cond_list))]
        )
      )
      (cond
        [(equal? (first conditions) "and") (set-intersect cond_list (rows_after_applying_conditions init_list (rest conditions) cond_list))]
        [(equal? (first conditions) "or") (set-union cond_list (rows_after_applying_conditions init_list (rest conditions) init_list))]
        [(equal? (first conditions) "not") (set-subtract (rest init_list) (rows_after_applying_conditions init_list (rest conditions) init_list))]
      ))
    )
  ))
)

(define (having lst expressions)
  (
    let*
    (
      [parsed_expressions (if expressions (split_condition_by_and/or/not expressions) #f)]
    )
    ;parsed_expressions
    (append (list (first lst)) (rows_after_applying_conditions lst parsed_expressions lst))
  )
)

(define (order_by selected_cols order_cols sort_word prev_order)
  (define (create_index_list selected_cols order_cols)
    (if (null? order_cols) '() (append (list (index-of (first selected_cols) (first order_cols))) (create_index_list selected_cols (rest order_cols))))
  )
  (define (sort_list selected_cols index_list sort_word prev_order)
    (define (compare a b) 
      (let*
      (
        [index (first index_list)]
        [a_number (string->number (list-ref a index))]
        [b_number (string->number (list-ref b index))]
        [a_prev (if prev_order (list-ref a prev_order) #f)]
        [b_prev (if prev_order (list-ref b prev_order) #f)]
      )
    (  
      if (equal? a_prev b_prev)
      (if (equal? sort_word "asc") 
        (if (and (number? a_number) (number? b_number)) (< a_number b_number) (string-ci<? (list-ref a index) (list-ref b index))) 
        (if (and (number? a_number) (number? b_number)) (> a_number b_number) (string-ci>? (list-ref a index) (list-ref b index)))
      ) #f
    ))
    )

    (if (null? index_list) selected_cols (sort_list (sort selected_cols compare) (rest index_list) sort_word (first index_list)))
  )

  (
  let*
  (
    [index_list (create_index_list selected_cols (if order_cols (remove-duplicates order_cols) '()))]
  )
  (append (list (first selected_cols)) (sort_list (rest selected_cols) index_list sort_word prev_order)))
)

(define (add_cases selected_cols cases case_indexes)
  (define (create_conds case)
    (
      let
      (
        [delimiter_index_start (index-of case "when")]
        [delimiter_index_finish (index-of case "then")]
      )
      (if (and delimiter_index_start delimiter_index_finish) 
        (append (list (slice case (add1 delimiter_index_start) delimiter_index_finish)) (create_conds (list-tail case (add1 delimiter_index_finish))))
        '()
      )
    ) 
  )
  (define (create_conds_values case)
    (
      let
      (
        [delimiter_index (or (index-of case "then") (index-of case "else"))]
      )
      (if delimiter_index 
        (append (list (list-ref case (add1 delimiter_index))) (create_conds_values (list-tail case (+ 2 delimiter_index))))
        '()
      )
    ) 
  )
  (define (apply_conditions selected_cols conditions)
    (if (null? conditions) '()
      (
        let*
        (
          [cols_names (first selected_cols)]
          [condition (first conditions)]
          [col_name (first condition)]
          [index_col_name (index-of cols_names col_name)]
          [cond_sign (second condition)]
          [value (third condition)]
        )
        (cond
          [(equal? cond_sign "<=") (append (list (map (lambda (selected_row) (<= (string->number (list-ref selected_row index_col_name)) (string->number value))) (rest selected_cols))) 
                                          (apply_conditions selected_cols (rest conditions)))]
          [(equal? cond_sign "<>") (append (list (map (lambda (selected_row) (not (equal? (list-ref selected_row index_col_name) value))) (rest selected_cols))) 
                                          (apply_conditions selected_cols (rest conditions)))]
          [(equal? cond_sign "<") (append (list (map (lambda (selected_row) (< (string->number (list-ref selected_row index_col_name)) (string->number value))) (rest selected_cols))) 
                                          (apply_conditions selected_cols (rest conditions)))]
          [(equal? cond_sign ">=") (append (list (map (lambda (selected_row) (>= (string->number (list-ref selected_row index_col_name)) (string->number value))) (rest selected_cols))) 
                                          (apply_conditions selected_cols (rest conditions)))]
          [(equal? cond_sign ">") (append (list (map (lambda (selected_row) (<= (string->number (list-ref selected_row index_col_name)) (string->number value))) (rest selected_cols))) 
                                          (apply_conditions selected_cols (rest conditions)))]
          [(equal? cond_sign "=") (append (list (map (lambda (selected_row) (equal? (list-ref selected_row index_col_name) value)) (rest selected_cols))) 
                                          (apply_conditions selected_cols (rest conditions)))]
        )
      )
    )
  )
  (
    let*
    (
      [cases_names (map (lambda (x) (last x)) cases)]
      [cases_conds (map (lambda (x) (create_conds x)) cases)]
      [cases_values (map (lambda (x) (create_conds_values x)) cases)]
      [truth_lists_for_cases (map (lambda (case) (transpose (apply_conditions selected_cols (create_conds case)))) cases)]
      [cases_lists (map (lambda (truth_list_for_case case_values case_name) 
                                (cons case_name (map (lambda (truth_list) (if (index-of truth_list #t) (list-ref case_values (index-of truth_list #t)) (last case_values))) truth_list_for_case))
                        ) truth_lists_for_cases cases_values cases_names
                   )
      ]
    )
    (transpose (foldl (lambda (index case_list result) (append-at result case_list index)) (transpose selected_cols) case_indexes cases_lists))
  )
  
)

(define (join left_table_list_of_rows right_table_list_of_rows col_left col_right kind_of_join)
  (define (create_result_indexes a b)
    (define (create_inner_join_indexes lst)
      (if (null? lst) '() (cons (list (first lst) (second lst)) (create_inner_join_indexes (cddr lst))))
    ) 
    (
      let*
      (
        [b_indexes (range (length b))]
        [lists_with_indexes_of_common 
          (filter-not null? (map (lambda (c) (if (null? (filter (lambda (index) (equal? c (list-ref b index))) b_indexes)) 
                                                '() 
                                                (cons (index-of a c) (filter (lambda (index) (equal? c (list-ref b index))) b_indexes)) 
                                             )
                                  ) a
                            )
          )
        ]
        [v (map (lambda (x) (cons (first x) (add-between (rest x) (first x)))) lists_with_indexes_of_common)]
      )
      (create_inner_join_indexes (append (first v) (second v)))
    )
  )
  (define (inner-join left_table_list_of_rows right_table_list_of_rows result_indexes)
    ( cons (append (first left_table_list_of_rows) (first right_table_list_of_rows))
      (map (lambda (lst) (append (list-ref left_table_list_of_rows (first lst)) (list-ref right_table_list_of_rows (second lst)))) result_indexes )
    )
  )
  (define (left-join left_table_list_of_rows right_table_list_of_rows result_indexes)
    (cons (append (first left_table_list_of_rows) (first right_table_list_of_rows))
      (map (lambda (elem)  
        (if (equal? (second elem) "") 
          (append (list-ref left_table_list_of_rows (first elem)) (make-list (length (first right_table_list_of_rows)) ""))
          (append (list-ref left_table_list_of_rows (first elem)) (list-ref right_table_list_of_rows (second elem)))
        ))
        (
          foldr append '() (rest (map (lambda (row_index) (if (null? (filter (lambda (x) (equal? (first x) row_index)) result_indexes)) 
                                                                  (list (list row_index "")) 
                                                                  (filter (lambda (x) (equal? (first x) row_index)) result_indexes)
                                                          ) 
                                      ) 
                                      (range (length left_table_list_of_rows))
                                 ) 
                           )
        )
      )
    )
  )
  (define (right-join left_table_list_of_rows right_table_list_of_rows result_indexes)
    (cons (append (first left_table_list_of_rows) (first right_table_list_of_rows))
      (map (lambda (elem)  
        (if (equal? (first elem) "") 
          (append (make-list (length (first left_table_list_of_rows)) "") (list-ref right_table_list_of_rows (second elem)) )
          (append (list-ref left_table_list_of_rows (first elem)) (list-ref right_table_list_of_rows (second elem)))
        )
          )
        (foldr append '() (rest (map (lambda (row_index) (if (null? (filter (lambda (x) (equal? (second x) row_index)) result_indexes)) 
                                                                  (list (list "" row_index)) 
                                                                  (filter (lambda (x) (equal? (second x) row_index)) result_indexes)
                                                         ) 
                                     ) 
                                     (range (length right_table_list_of_rows))
                                ) 
                          )
        )
      )
    ) 
  )
  (define (full-outer-join left_table_list_of_rows right_table_list_of_rows result_indexes)
    (remove-duplicates
      (append (left-join left_table_list_of_rows right_table_list_of_rows result_indexes) 
              (right-join left_table_list_of_rows right_table_list_of_rows result_indexes)
      )
    )
  )
  (if kind_of_join 
    (let*
      (
        [result_indexes (create_result_indexes (list-ref (transpose left_table_list_of_rows) (index-of (first left_table_list_of_rows) col_left))
            (list-ref (transpose right_table_list_of_rows) (index-of (first right_table_list_of_rows) col_right)) )
        ]
      )
      (cond 
        [(equal? kind_of_join "inner") (inner-join left_table_list_of_rows right_table_list_of_rows result_indexes)] 
        [(equal? kind_of_join "left") (left-join left_table_list_of_rows right_table_list_of_rows result_indexes)]
        [(equal? kind_of_join "right") (right-join left_table_list_of_rows right_table_list_of_rows result_indexes)]
        [(equal? kind_of_join "outer") (full-outer-join left_table_list_of_rows right_table_list_of_rows result_indexes)]
      )
    )
    left_table_list_of_rows
  )

)
(define (exec_query left_table_list_of_rows right_table_list_of_rows col_left col_right kind_of_join sel_cols 
    conditions is_distinct order_columns order_sort_word group_columns having_columns) 
  (define (all_selected_expessions sel_cols)
    (if (null? sel_cols) '() 
      (if (equal? (first sel_cols) "case")
        (append (list (slice sel_cols 1 (+ 2 (index-of sel_cols "as")))) (all_selected_expessions (list-tail sel_cols (+ 2 (index-of sel_cols "as")))))
        (cons (first sel_cols) (all_selected_expessions (rest sel_cols)))
      )
    )
  )
  (define (create_cases_indexes list_of_expressions joined_table acc result)
    (if (null? list_of_expressions) result
      (if (list? (first list_of_expressions)) 
        (create_cases_indexes (rest list_of_expressions) joined_table (add1 acc) (append result (list acc)))
        (create_cases_indexes (rest list_of_expressions) joined_table (if (equal? (first list_of_expressions) "*") (+ (length (first joined_table)) acc) (add1 acc)) result)
      )
    )
  )
 (
   let*
   (
     [joined_table (join left_table_list_of_rows right_table_list_of_rows col_left col_right kind_of_join)]
     [parsed_conditions (if conditions (split_condition_by_and/or/not conditions) #f)]
     [list_after_conditions (append (list (first joined_table)) (rows_after_applying_conditions joined_table parsed_conditions joined_table))]
     [all_select_expressions (all_selected_expessions sel_cols)]
     [cases (filter list? all_select_expressions)]
     [case_indexes (create_cases_indexes all_select_expressions joined_table 0 '())]
     [selected_expressions_without_cases (filter string? all_select_expressions)]
     [selected_cols (select list_after_conditions selected_expressions_without_cases group_columns)]
     [selected_cols_with_cases (add_cases selected_cols cases case_indexes)]
     [cols_after_having (having selected_cols_with_cases having_columns)]
     [distinct_selected_cols (distinct is_distinct cols_after_having)]
     [ordered_cols (order_by distinct_selected_cols order_columns order_sort_word #f)]
     [result ordered_cols]  
   )
   result
 )
)

(define (query_processing query) 
  (
    let*
    (
      [words (filter-not (lambda (x) (equal? x "")) (string-split query #px"\\s+|,")) ]
      [tables (filter (lambda (x) (or (string-suffix? x ".csv") (string-suffix? x ".tsv"))) words)]
      [join_on_index (index-of words "on")]
      [left_table_list_of_rows (lists (first tables))]
      [right_table_list_of_rows (if (> (length tables) 1) (lists (second tables)) #f)]
      [col_left (if join_on_index (last (string-split (list-ref words (add1 join_on_index)) ".")) #f)]
      [col_right (if join_on_index (last (string-split (list-ref words (+ 3 join_on_index)) ".")) #f)]
      [kind_of_join (if join_on_index (list-ref words(- join_on_index 3)) #f)]
      [start_index (if (index-of words "select") (add1 (index-of words "select")) #f)]
      [is_distinct (if start_index (equal? (list-ref words start_index) "distinct") #f)]
      [finish_index (index-of words "from")]
      [sel_cols (if start_index (map (lambda (x) (substring x (if (string-contains? x ".") (add1 (last (indexes-of (string->list x) #\.))) 0 ) (string-length x)) )
        (slice words (if is_distinct (add1 start_index) start_index) finish_index)) '())]
      [where_index (index-of words "where")]
      [group_index (index-of words "group")]
      [order_index (index-of words "order")] 
      [having_index (index-of words "having")]
      [conditions (if where_index (slice words (add1 where_index) (if group_index group_index (if order_index order_index (length words)))) #f) ]
      [group_columns (if group_index (slice words (+ 2 group_index) (if having_index having_index (if order_index order_index (length words)))) #f)]
      [having_columns (if (and group_index having_index) (slice words (add1 having_index) (if order_index order_index (length words))) #f)]
      [order_columns_with_sort_word (if order_index (slice words (+ 2 order_index) (length words)) #f)]
      [order_sort_word (if order_columns_with_sort_word (if (equal? (last order_columns_with_sort_word) "asc") "asc" "desc") #f)]
      [order_columns (if order_columns_with_sort_word (take order_columns_with_sort_word (sub1 (length order_columns_with_sort_word))) #f)]
    ) 
    (if (equal? (first words) "load") left_table_list_of_rows
    (exec_query left_table_list_of_rows right_table_list_of_rows col_left col_right kind_of_join sel_cols 
    conditions is_distinct order_columns order_sort_word group_columns having_columns)
  ))
)

(define (make_print a) (if (null? a) null (begin (writeln (first a)) (make_print (rest a)))) )

(define (entry query) 
  (if (equal? query 'quit) 
    "End of the CLI" 
    (begin (make_print (query_processing query)) (display "Enter the query: ") (entry (read))))
)

(display "Enter the query: ")
(entry (read))




;(display (lists "map_zal-skl9.csv"))