package it.costantino.astaapp.web.rest;

import it.costantino.astaapp.repository.LegaRepository;
import it.costantino.astaapp.service.LegaService;
import it.costantino.astaapp.service.dto.LegaDTO;
import it.costantino.astaapp.web.rest.errors.BadRequestAlertException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.jhipster.web.util.HeaderUtil;
import tech.jhipster.web.util.ResponseUtil;

/**
 * REST controller for managing {@link it.costantino.astaapp.domain.Lega}.
 */
@RestController
@RequestMapping("/api/legas")
public class LegaResource {

    private static final Logger LOG = LoggerFactory.getLogger(LegaResource.class);

    private static final String ENTITY_NAME = "lega";

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final LegaService legaService;

    private final LegaRepository legaRepository;

    public LegaResource(LegaService legaService, LegaRepository legaRepository) {
        this.legaService = legaService;
        this.legaRepository = legaRepository;
    }

    /**
     * {@code POST  /legas} : Create a new lega.
     *
     * @param legaDTO the legaDTO to create.
     * @return the {@link ResponseEntity} with status {@code 201 (Created)} and with body the new legaDTO, or with status {@code 400 (Bad Request)} if the lega has already an ID.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PostMapping("")
    public ResponseEntity<LegaDTO> createLega(@Valid @RequestBody LegaDTO legaDTO) throws URISyntaxException {
        LOG.debug("REST request to save Lega : {}", legaDTO);
        if (legaDTO.getId() != null) {
            throw new BadRequestAlertException("A new lega cannot already have an ID", ENTITY_NAME, "idexists");
        }
        legaDTO = legaService.save(legaDTO);
        return ResponseEntity.created(new URI("/api/legas/" + legaDTO.getId()))
            .headers(HeaderUtil.createEntityCreationAlert(applicationName, false, ENTITY_NAME, legaDTO.getId().toString()))
            .body(legaDTO);
    }

    /**
     * {@code PUT  /legas/:id} : Updates an existing lega.
     *
     * @param id the id of the legaDTO to save.
     * @param legaDTO the legaDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated legaDTO,
     * or with status {@code 400 (Bad Request)} if the legaDTO is not valid,
     * or with status {@code 500 (Internal Server Error)} if the legaDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PutMapping("/{id}")
    public ResponseEntity<LegaDTO> updateLega(
        @PathVariable(value = "id", required = false) final Long id,
        @Valid @RequestBody LegaDTO legaDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to update Lega : {}, {}", id, legaDTO);
        if (legaDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, legaDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!legaRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        legaDTO = legaService.update(legaDTO);
        return ResponseEntity.ok()
            .headers(HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, legaDTO.getId().toString()))
            .body(legaDTO);
    }

    /**
     * {@code PATCH  /legas/:id} : Partial updates given fields of an existing lega, field will ignore if it is null
     *
     * @param id the id of the legaDTO to save.
     * @param legaDTO the legaDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated legaDTO,
     * or with status {@code 400 (Bad Request)} if the legaDTO is not valid,
     * or with status {@code 404 (Not Found)} if the legaDTO is not found,
     * or with status {@code 500 (Internal Server Error)} if the legaDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PatchMapping(value = "/{id}", consumes = { "application/json", "application/merge-patch+json" })
    public ResponseEntity<LegaDTO> partialUpdateLega(
        @PathVariable(value = "id", required = false) final Long id,
        @NotNull @RequestBody LegaDTO legaDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to partial update Lega partially : {}, {}", id, legaDTO);
        if (legaDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, legaDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!legaRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        Optional<LegaDTO> result = legaService.partialUpdate(legaDTO);

        return ResponseUtil.wrapOrNotFound(
            result,
            HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, legaDTO.getId().toString())
        );
    }

    /**
     * {@code GET  /legas} : get all the legas.
     *
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and the list of legas in body.
     */
    @GetMapping("")
    public List<LegaDTO> getAllLegas() {
        LOG.debug("REST request to get all Legas");
        return legaService.findAll();
    }

    /**
     * {@code GET  /legas/:id} : get the "id" lega.
     *
     * @param id the id of the legaDTO to retrieve.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the legaDTO, or with status {@code 404 (Not Found)}.
     */
    @GetMapping("/{id}")
    public ResponseEntity<LegaDTO> getLega(@PathVariable("id") Long id) {
        LOG.debug("REST request to get Lega : {}", id);
        Optional<LegaDTO> legaDTO = legaService.findOne(id);
        return ResponseUtil.wrapOrNotFound(legaDTO);
    }

    /**
     * {@code DELETE  /legas/:id} : delete the "id" lega.
     *
     * @param id the id of the legaDTO to delete.
     * @return the {@link ResponseEntity} with status {@code 204 (NO_CONTENT)}.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteLega(@PathVariable("id") Long id) {
        LOG.debug("REST request to delete Lega : {}", id);
        legaService.delete(id);
        return ResponseEntity.noContent()
            .headers(HeaderUtil.createEntityDeletionAlert(applicationName, false, ENTITY_NAME, id.toString()))
            .build();
    }
}
