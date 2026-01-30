{% macro get_review_score_description(review_score) %}

    case {{ dbt.safe_cast("review_score", api.Column.translate_type("integer")) }} 
        when 1 then 'Very Dissatisfied'
        when 2 then 'Dissatisfied'
        when 3 then 'Neutral'
        when 4 then 'Satisfied'
        when 5 then 'Very Satisfied'
        else 'no review'
    end
{% endmacro %}
